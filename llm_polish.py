"""
LLM Polish Pass - Final sanity checks on dossier data.

Uses GPT5-nano to:
1. Validate if contact names are real human names
2. For Chinese rep candidates, identify Chinese staff as primary contact
3. Clean up any obvious issues

Usage:
    python llm_polish.py [--dry-run] [--limit N]
"""

import argparse
import json
import os
import re
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path

import requests

# Load env
env_vars = {}
try:
    with open('.env', 'r') as f:
        for line in f:
            if '=' in line and not line.startswith('#'):
                k, v = line.strip().split('=', 1)
                env_vars[k] = v.strip('"').strip("'")
except:
    pass

OPENAI_API_KEY = env_vars.get('OPENAI_API_KEY') or os.getenv('OPENAI_API_KEY')
OPENAI_MODEL = env_vars.get('OPENAI_MODEL', 'gpt-4o-mini')
SUPABASE_URL = env_vars.get('SUPABASE_URL') or os.getenv('SUPABASE_URL')
SUPABASE_KEY = env_vars.get('SUPABASE_KEY') or env_vars.get('SUPABASE_ANON_KEY') or os.getenv('SUPABASE_KEY')


def call_llm(prompt: str, system: str = None) -> str:
    """Call OpenAI API."""
    messages = []
    if system:
        messages.append({"role": "system", "content": system})
    messages.append({"role": "user", "content": prompt})

    resp = requests.post(
        "https://api.openai.com/v1/chat/completions",
        headers={
            "Authorization": f"Bearer {OPENAI_API_KEY}",
            "Content-Type": "application/json"
        },
        json={
            "model": OPENAI_MODEL,
            "messages": messages,
            "temperature": 0,
            "max_tokens": 500
        },
        timeout=30
    )
    resp.raise_for_status()
    return resp.json()["choices"][0]["message"]["content"]


SYSTEM_PROMPT = """You are a data quality assistant. Your job is to validate and clean contact information.

RULES:
1. Only return valid human names (First Last format)
2. REJECT organization names, program names, locations, titles without names
3. For Chinese rep candidates, identify the Chinese staff member by surname
4. Return JSON only, no explanations

Common Chinese surnames: Chan, Chen, Cheung, Chow, Chu, Fung, Ho, Huang, Lam, Lee, Leung, Li, Lin, Liu, Lo, Mak, Ng, Tang, Tse, Wong, Wu, Yang, Yip, Yu, Zhang, Zhao, Zhou"""


def polish_lead(lead: dict) -> dict:
    """Polish a single lead with LLM validation."""
    lead_id = lead.get('id')
    name = lead.get('name', '')
    contact_name = lead.get('contact_name', '')
    contact_role = lead.get('contact_role', '')
    decision_makers = lead.get('decision_makers', [])
    chinese_rep = lead.get('chinese_rep_candidate', False)
    chinese_reasons = lead.get('chinese_rep_reasons', [])
    sales_brief = lead.get('sales_brief', '')

    # Build prompt
    prompt = f"""Facility: {name}

Current contact_name: {contact_name}
Current contact_role: {contact_role}
Decision makers list: {json.dumps(decision_makers)}
Chinese rep candidate: {chinese_rep}
Chinese rep reasons: {json.dumps(chinese_reasons)}

Tasks:
1. Is contact_name a valid human name? (not org name, not program, not location)
2. If invalid, find best valid name from decision_makers list
3. If chinese_rep=true, identify the Chinese staff member and make them primary contact

Return JSON:
{{
  "contact_name_valid": true/false,
  "new_contact_name": "name or null",
  "new_contact_role": "role or null",
  "chinese_staff_name": "name if chinese_rep else null",
  "chinese_staff_role": "role if chinese_rep else null"
}}"""

    try:
        response = call_llm(prompt, SYSTEM_PROMPT)
        # Extract JSON from response
        json_match = re.search(r'\{[^}]+\}', response, re.DOTALL)
        if json_match:
            result = json.loads(json_match.group())
            return {
                'id': lead_id,
                'name': name,
                'original_contact': contact_name,
                'result': result
            }
    except Exception as e:
        return {
            'id': lead_id,
            'name': name,
            'error': str(e)
        }

    return {'id': lead_id, 'name': name, 'error': 'No result'}


def fetch_leads_from_supabase() -> list:
    """Fetch leads from Supabase."""
    resp = requests.get(
        f'{SUPABASE_URL}/rest/v1/leads?select=*',
        headers={
            'apikey': SUPABASE_KEY,
            'Authorization': f'Bearer {SUPABASE_KEY}'
        }
    )
    resp.raise_for_status()
    return resp.json()


def update_lead_in_supabase(lead_id: int, updates: dict) -> bool:
    """Update a lead in Supabase."""
    resp = requests.patch(
        f'{SUPABASE_URL}/rest/v1/leads?id=eq.{lead_id}',
        headers={
            'apikey': SUPABASE_KEY,
            'Authorization': f'Bearer {SUPABASE_KEY}',
            'Content-Type': 'application/json',
            'Prefer': 'return=minimal'
        },
        json=updates
    )
    return resp.status_code in (200, 204)


def main():
    parser = argparse.ArgumentParser(description="LLM Polish Pass")
    parser.add_argument("--dry-run", action="store_true", help="Don't update database")
    parser.add_argument("--limit", type=int, default=0, help="Limit number of leads to process")
    parser.add_argument("--chinese-only", action="store_true", help="Only process Chinese rep candidates")
    args = parser.parse_args()

    if not OPENAI_API_KEY:
        print("Error: Set OPENAI_API_KEY in .env")
        return

    # Fetch leads
    print("Fetching leads from Supabase...")
    leads = fetch_leads_from_supabase()
    print(f"Fetched {len(leads)} leads")

    # Filter if requested
    if args.chinese_only:
        leads = [l for l in leads if l.get('chinese_rep_candidate')]
        print(f"Filtered to {len(leads)} Chinese rep candidates")

    if args.limit:
        leads = leads[:args.limit]
        print(f"Limited to {len(leads)} leads")

    if not leads:
        print("No leads to process")
        return

    # Process in parallel
    print(f"\nProcessing {len(leads)} leads with {OPENAI_MODEL}...")
    results = []
    updates_to_apply = []

    with ThreadPoolExecutor(max_workers=10) as executor:
        futures = {executor.submit(polish_lead, lead): lead['id'] for lead in leads}

        for i, future in enumerate(as_completed(futures)):
            result = future.result()
            results.append(result)

            if (i + 1) % 10 == 0:
                print(f"  Progress: {i + 1}/{len(leads)}")

            # Determine updates
            if 'error' not in result and result.get('result'):
                r = result['result']
                updates = {}

                # If current contact is invalid and we have a new one
                if not r.get('contact_name_valid') and r.get('new_contact_name'):
                    updates['contact_name'] = r['new_contact_name']
                    updates['contact_role'] = r.get('new_contact_role')

                # If Chinese rep and we identified Chinese staff
                if r.get('chinese_staff_name'):
                    updates['contact_name'] = r['chinese_staff_name']
                    updates['contact_role'] = r.get('chinese_staff_role')

                if updates:
                    updates_to_apply.append({
                        'id': result['id'],
                        'name': result['name'],
                        'original': result.get('original_contact'),
                        'updates': updates
                    })

    # Summary
    print(f"\n=== Results ===")
    print(f"Processed: {len(results)}")
    print(f"Updates needed: {len(updates_to_apply)}")

    if updates_to_apply:
        print("\nUpdates:")
        for u in updates_to_apply[:20]:
            print(f"  {u['name'][:35]:35} '{u['original']}' -> '{u['updates'].get('contact_name')}'")
        if len(updates_to_apply) > 20:
            print(f"  ... and {len(updates_to_apply) - 20} more")

    if args.dry_run:
        print("\n[DRY RUN - No changes made]")
        return

    # Apply updates
    print(f"\nApplying {len(updates_to_apply)} updates to Supabase...")
    success = 0
    for u in updates_to_apply:
        if update_lead_in_supabase(u['id'], u['updates']):
            success += 1

    print(f"Updated: {success}/{len(updates_to_apply)}")


if __name__ == "__main__":
    main()
