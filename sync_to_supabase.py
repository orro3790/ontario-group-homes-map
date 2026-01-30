"""
Sync enriched dossier data to Supabase with geocoding.

Usage:
    python sync_to_supabase.py [--input data/enriched/leads.jsonl] [--dry-run] [--skip-geocoding]
"""

import argparse
import json
import os
import re
import time
from pathlib import Path
from concurrent.futures import ThreadPoolExecutor, as_completed

try:
    from supabase import create_client, Client
except ImportError:
    print("Install supabase: pip install supabase")
    exit(1)

try:
    from geopy.geocoders import Nominatim, ArcGIS
    from geopy.extra.rate_limiter import RateLimiter
    HAS_GEOPY = True
except ImportError:
    HAS_GEOPY = False

try:
    from dotenv import load_dotenv
    load_dotenv()
except ImportError:
    pass


def strip_citation_hashes(text: str) -> str:
    """Remove [hash] citation markers from text."""
    if not text:
        return text
    # Remove patterns like [40-char hex hash]
    return re.sub(r'\s*\[[a-f0-9]{40}\]', '', text).strip()


def geocode_single(args: tuple) -> tuple[int, float | None, float | None]:
    """Geocode a single address. Returns (index, lat, lon)."""
    idx, address, geolocator = args

    if not address:
        return (idx, None, None)

    # Clean up address for geocoding
    search_addr = address
    if 'ON' not in address.upper() and 'Ontario' not in address:
        search_addr = f"{address}, Ontario, Canada"
    elif 'Canada' not in address:
        search_addr = f"{address}, Canada"

    try:
        location = geolocator.geocode(search_addr, timeout=10)
        if location:
            return (idx, location.latitude, location.longitude)
    except Exception:
        pass
    return (idx, None, None)


def geocode_rows(rows: list[dict], skip_existing: bool = True) -> list[dict]:
    """Add lat/lon to rows using parallel ArcGIS geocoding."""
    if not HAS_GEOPY:
        print("Warning: geopy not installed, skipping geocoding")
        return rows

    # Use ArcGIS - faster, no strict rate limit, no API key needed
    geolocator = ArcGIS(timeout=10)

    to_geocode = []
    for i, row in enumerate(rows):
        if skip_existing and row.get("lat") and row.get("lon"):
            continue
        if row.get("address"):
            to_geocode.append((i, row["address"], geolocator))

    if not to_geocode:
        print("All rows already have coordinates or no addresses")
        return rows

    print(f"Geocoding {len(to_geocode)} addresses with ArcGIS (parallel)...")

    success = 0
    completed = 0

    # Use 10 parallel workers
    with ThreadPoolExecutor(max_workers=10) as executor:
        futures = {executor.submit(geocode_single, args): args[0] for args in to_geocode}

        for future in as_completed(futures):
            try:
                idx, lat, lon = future.result()
                if lat and lon:
                    rows[idx]["lat"] = lat
                    rows[idx]["lon"] = lon
                    success += 1
                completed += 1

                if completed % 50 == 0:
                    print(f"  Progress: {completed}/{len(to_geocode)} ({success} successful)")

            except Exception as e:
                completed += 1

    print(f"Geocoded {success}/{len(to_geocode)} addresses")
    return rows


def load_dossiers(path: Path) -> list[dict]:
    """Load dossiers from JSONL file."""
    if not path.exists():
        return []
    dossiers = []
    with path.open("r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if line:
                try:
                    dossiers.append(json.loads(line))
                except json.JSONDecodeError:
                    continue
    return dossiers


def score_to_priority(score: int) -> str:
    """Convert score to priority level."""
    if score is None:
        return "medium"
    if score >= 50:
        return "urgent"
    if score >= 40:
        return "high"
    if score >= 30:
        return "medium"
    return "low"


def is_valid_person_name(name: str) -> bool:
    """Check if a string looks like a real person's name."""
    if not name:
        return False
    name = name.strip()
    words = name.split()

    # Must have 2-4 words (first + last, maybe middle)
    if len(words) < 2 or len(words) > 5:
        return False

    # Not too long
    if len(name) > 40:
        return False

    # Filter out obvious garbage patterns
    garbage_patterns = [
        'place', 'house', 'home', 'lodge', 'manor', 'centre', 'center',
        'residence', 'hope', 'care', 'services', 'program', 'health',
        'living', 'community', 'support', 'society', 'international',
        'fellowship', 'association', 'foundation', 'organization', 'org',
        'scarborough', 'toronto', 'ottawa', 'hamilton', 'london', 'ontario',
        'north', 'south', 'east', 'west', 'central',
        'next level', 'the mind', 'action canada', 'rotary',
        'shelter', 'youth', 'housing', 'after-care', 'mental', 'addiction',
        'seniors', 'elderly', 'assisted', 'nursing', 'rehab', 'recovery',
        'clinic', 'hospital', 'medical', 'wellness', 'outreach',
        'ministry', 'government', 'provincial', 'federal', 'municipal',
        'inc', 'ltd', 'corp', 'llc', 'limited',
    ]
    name_lower = name.lower()
    if any(pattern in name_lower for pattern in garbage_patterns):
        return False

    # Each word should be capitalized properly (first letter upper, rest lower/mixed)
    for word in words:
        if len(word) <= 1:
            continue
        if not word[0].isupper():
            return False

    return True


def clean_decision_makers(dms: list) -> list:
    """Filter out garbage decision makers."""
    cleaned = []
    for dm in dms:
        if not isinstance(dm, dict):
            continue
        name = dm.get("name", "")
        if is_valid_person_name(name):
            cleaned.append(dm)
    return cleaned


def clean_talking_points(points: list) -> list:
    """Strip citation hashes from talking points."""
    cleaned = []
    for p in points:
        if isinstance(p, dict):
            cleaned_point = dict(p)
            if "point" in cleaned_point:
                cleaned_point["point"] = strip_citation_hashes(cleaned_point["point"])
            cleaned.append(cleaned_point)
        elif isinstance(p, str):
            cleaned.append(strip_citation_hashes(p))
        else:
            cleaned.append(p)
    return cleaned


def clean_chinese_rep_fit(fit: dict, valid_dms: list) -> dict:
    """Re-evaluate Chinese rep fit based on cleaned decision makers."""
    if not fit or not fit.get("is_candidate"):
        return fit

    # Get valid person names
    valid_names = {dm.get("name", "").lower() for dm in valid_dms}

    # Filter reasons to only include those referencing valid names
    cleaned_reasons = []
    for reason in fit.get("reasons", []):
        detail = reason.get("detail", "").lower()
        # Check if any valid name is mentioned in the reason
        if any(name in detail for name in valid_names if name):
            cleaned_reasons.append(reason)

    # If no valid reasons remain, downgrade the fit
    if not cleaned_reasons:
        return {
            "is_candidate": False,
            "confidence": "none",
            "reasons": [],
        }

    return {
        "is_candidate": True,
        "confidence": fit.get("confidence", "low"),
        "reasons": cleaned_reasons,
    }


def dossier_to_row(d: dict) -> dict:
    """Convert dossier to Supabase row format."""
    # Clean decision makers and get primary contact
    dms = clean_decision_makers(d.get("decision_makers", []))
    primary_dm = dms[0] if dms else {}

    # Re-evaluate Chinese rep fit based on valid decision makers
    fit = clean_chinese_rep_fit(d.get("chinese_rep_fit", {}), dms)

    return {
        "lead_id": d.get("lead_id"),
        "name": d.get("name"),
        "address": d.get("address"),
        "phone": d.get("phone"),
        "city": d.get("city"),
        "website": d.get("website") or d.get("listing_url"),
        "source": "dossier_pipeline",

        # Enrichment scores
        "score": d.get("overall_priority", 0),
        "priority": score_to_priority(d.get("overall_priority", 0)),
        "overall_priority": d.get("overall_priority"),
        "independence_score": d.get("independence_score"),
        "contactability_score": d.get("contactability_score"),
        "pharma_fit_score": d.get("pharma_fit_score"),
        "partnership_openness_score": d.get("partnership_openness_score"),
        "capacity_score": d.get("capacity_score"),
        "sales_brief": strip_citation_hashes(d.get("sales_brief", "")),

        # Dossier content as JSON
        "decision_makers": dms,
        "services_offered": d.get("services_offered", []),
        "talking_points": clean_talking_points(d.get("talking_points", [])),
        "resident_populations": d.get("resident_populations", []),
        "medication_signals": d.get("medication_management_signals", []),
        "partnerships": d.get("partnerships_and_affiliations", []),
        "next_step": d.get("next_step", {}),

        # Primary contact
        "contact_name": primary_dm.get("name"),
        "contact_email": primary_dm.get("email"),
        "contact_role": primary_dm.get("title"),

        # Language & Chinese rep
        "languages_supported": d.get("languages_supported", []),
        "chinese_rep_candidate": fit.get("is_candidate", False),
        "chinese_rep_confidence": fit.get("confidence", "none"),
        "chinese_rep_reasons": fit.get("reasons", []),

        # Coordinates (will be filled by geocoding)
        "lat": d.get("lat"),
        "lon": d.get("lon") or d.get("lon"),
    }


def main():
    parser = argparse.ArgumentParser(description="Sync dossiers to Supabase")
    parser.add_argument("--input", default="data/enriched/leads.jsonl")
    parser.add_argument("--dry-run", action="store_true", help="Print what would be synced without writing")
    parser.add_argument("--skip-geocoding", action="store_true", help="Skip geocoding step")
    args = parser.parse_args()

    # Check env vars
    url = os.getenv("SUPABASE_URL")
    key = os.getenv("SUPABASE_KEY") or os.getenv("SUPABASE_ANON_KEY")

    if not url or not key:
        print("Error: Set SUPABASE_URL and SUPABASE_KEY (or SUPABASE_ANON_KEY) environment variables")
        print("You can add these to a .env file")
        return

    # Load dossiers
    input_path = Path(args.input)
    dossiers = load_dossiers(input_path)

    if not dossiers:
        print(f"No dossiers found at {input_path}")
        return

    print(f"Loaded {len(dossiers)} dossiers")

    # Convert to rows
    rows = [dossier_to_row(d) for d in dossiers]

    # Geocode addresses
    if not args.skip_geocoding:
        rows = geocode_rows(rows)

    # Count stats
    chinese_candidates = sum(1 for r in rows if r["chinese_rep_candidate"])
    with_coords = sum(1 for r in rows if r.get("lat") and r.get("lon"))
    print(f"Chinese rep candidates: {chinese_candidates}")
    print(f"With coordinates: {with_coords}/{len(rows)}")

    if args.dry_run:
        print("\n=== DRY RUN - Would sync these leads: ===")
        for row in rows[:5]:
            print(f"  - {row['name']} (chinese_rep={row['chinese_rep_candidate']}, conf={row['chinese_rep_confidence']})")
        if len(rows) > 5:
            print(f"  ... and {len(rows) - 5} more")
        return

    # Connect to Supabase
    supabase: Client = create_client(url, key)

    # Upsert rows (insert or update based on lead_id)
    print(f"\nSyncing {len(rows)} leads to Supabase...")

    success = 0
    errors = []

    for row in rows:
        try:
            # Upsert based on lead_id
            result = supabase.table("leads").upsert(
                row,
                on_conflict="lead_id"
            ).execute()
            success += 1
        except Exception as e:
            errors.append((row.get("name", "unknown"), str(e)))

    print(f"\nSync complete:")
    print(f"  Success: {success}")
    print(f"  Errors: {len(errors)}")

    if errors:
        print("\nErrors:")
        for name, error in errors[:10]:
            print(f"  - {name}: {error}")
        if len(errors) > 10:
            print(f"  ... and {len(errors) - 10} more errors")


if __name__ == "__main__":
    main()
