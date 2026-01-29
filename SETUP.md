# Ontario Group Homes CRM - Setup Guide

## Quick Start

### Step 1: Create Supabase Project
1. Go to [supabase.com](https://supabase.com) and create account
2. Click **New Project**
3. Name: `ontario-crm`
4. Set database password (save it!)
5. Region: US East
6. Wait ~2 minutes for provisioning

### Step 2: Create Database Tables
1. In Supabase, go to **SQL Editor**
2. Click **New Query**
3. Paste contents of `supabase_schema.sql`
4. Click **Run**

### Step 3: Get Your API Keys
1. Go to **Settings** → **API**
2. Copy:
   - **Project URL**: `https://xxxxx.supabase.co`
   - **anon public key**: for the web app
   - **service_role key**: for import script only

### Step 4: Import Your Leads
```bash
# Edit import_to_supabase.py with your URL and service_role key
python import_to_supabase.py
```

### Step 5: Configure Web App
Edit `crm.html` lines ~480:
```javascript
const SUPABASE_URL = 'https://xxxxx.supabase.co';
const SUPABASE_ANON_KEY = 'your-anon-public-key';
```

### Step 6: Deploy
```bash
git add crm.html supabase_schema.sql SETUP.md .gitignore
git commit -m "Add CRM app"
git push
```

Access at: `https://orro3790.github.io/ontario-group-homes-map/crm.html`

---

## CRM Features

### Pipeline View (Kanban)
- Drag-and-drop style pipeline stages
- Stages: New → Contacted → Qualified → Proposal → Negotiation → Won/Lost
- Color-coded priority indicators
- Follow-up date tracking with overdue alerts

### Map View
- Interactive map of all 358 leads
- Color-coded markers by pipeline stage
- Filter by status and priority
- Search by name or city
- Click markers to open lead details

### Lead Management
- **Contact Info**: Phone, contact name, role, email
- **Status Tracking**: Pipeline stage, priority level
- **Follow-up Dates**: Schedule next actions
- **Estimated Value**: Track deal size
- **Notes**: Free-form notes per lead

### Activity Logging
- Log calls, emails, meetings
- Create follow-up tasks with due dates
- Full activity timeline per lead
- Auto-updates "last contacted" date

### Tasks View
- Centralized task list across all leads
- Due date tracking with overdue indicators
- One-click task completion
- Links to associated leads

### Export
- Export all leads to CSV
- Includes all CRM fields
- Compatible with Excel/Google Sheets

---

## Security

- **Row Level Security (RLS)** enabled
- Only authenticated users can access data
- `anon` key is safe to expose (designed for client-side)
- `service_role` key stays local (never commit it)

---

## Files

| File | Purpose |
|------|---------|
| `crm.html` | Full CRM web application |
| `supabase_schema.sql` | Database schema (leads + activities) |
| `import_to_supabase.py` | One-time data import script |
| `index.html` | Original public map (no CRM) |
| `.gitignore` | Excludes sensitive files |
