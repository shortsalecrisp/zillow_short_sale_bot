import re, requests
from bs4 import BeautifulSoup
import time

# helper – pull description from listing HTML if API field empty
def fetch_zillow_description(detail_url: str) -> str:
    try:
        resp = requests.get(detail_url, timeout=10, headers={
            "User-Agent": "Mozilla/5.0 (compatible; ShortSaleBot/1.0)"
        })
        resp.raise_for_status()
    except Exception:
        return ""

    soup = BeautifulSoup(resp.text, "html.parser")

    # 1) Zillow often stores JSON state in scripts – look there first
    for script in soup.find_all("script", type="application/json"):
        if "homeDescription" in script.string or "descriptionPlainText" in script.string:
            m = re.search(r'"(?:homeDescription|descriptionPlainText)"\s*:\s*"([^"]+)"',
                          script.string)
            if m:
                return bytes(m.group(1), "utf-8").decode("unicode_escape")

    # 2) fallback: look for the visible “What’s special …” section
    el = soup.find(string=re.compile(r"(?i)what.?s.+special")).find_parent("section") \
         if soup.find(string=re.compile(r"(?i)what.?s.+special")) else None
    if el:
        return " ".join(el.stripped_strings)

    return ""

def process_rows(rows):
    print(f"► fetched {len(rows)} rows at {time.strftime('%X')}", flush=True)

    conn = sqlite3.connect("seen.db")
    conn.execute("CREATE TABLE IF NOT EXISTS listings (zpid TEXT PRIMARY KEY)")
    conn.commit()

    for row in rows:
        zpid = str(row.get("zpid", ""))
        if conn.execute("SELECT 1 FROM listings WHERE zpid=?", (zpid,)).fetchone():
            continue

        listing_text = (
            row.get("homeDescription")
            or row.get("description")
            or row.get("hdpData", {}).get("homeInfo", {}).get("homeDescription")
            or ""
        )

        if not listing_text:
            detail_url = row.get("detailUrl") or row.get("url") or ""
            listing_text = fetch_zillow_description(detail_url) if detail_url else ""

        if not listing_text:
            print(f"⤷ skip {zpid}: still no description")
            continue

        filter_prompt = (
            "Return YES if the following listing text contains the exact phrase "
            "'short sale' (case-insensitive) **and** does NOT contain any of these "
            "disqualifying words: approved, negotiator, settlement fee, fee at closing. "
            "Otherwise return NO.\n\n"
            f"{listing_text}"
        )

        resp = client.chat.completions.create(
            model="gpt-3.5-turbo-0125",
            messages=[{"role": "user", "content": filter_prompt}],
            temperature=0.0,
        )
        decision = resp.choices[0].message.content.strip().upper()
        if not decision.startswith("YES"):
            continue

        agent_name = row.get("listingAgent", {}).get("name", "")
        state      = row.get("addressState") or row.get("state", "")
        contact_prompt = (
            f"Find the MOBILE phone number and email for real-estate agent "
            f"{agent_name} in {state}. Respond in JSON with keys 'phone' and 'email'."
        )

        cont_resp = client.chat.completions.create(
            model="gpt-3.5-turbo-0125",
            messages=[{"role": "user", "content": contact_prompt}],
            temperature=0.2,
        )
        try:
            contact = json.loads(cont_resp.choices[0].message.content)
            phone   = contact.get("phone", "").strip()
            email   = contact.get("email", "").strip()
        except Exception:
            phone = email = ""

        if not phone:
            continue  # can’t proceed without a number

        first, *rest = agent_name.split()
        last = " ".join(rest)
        address = row.get("address") or row.get("addressStreet") or ""
        city    = row.get("addressCity") or ""
        st      = row.get("addressState") or row.get("state") or ""

        SHEET.append_row([first, last, phone, email, address, city, st, "", "", ""])

        # ── STEP 5: mark as seen & optional SMS logic ──────────
        conn.execute("INSERT OR IGNORE INTO listings(zpid) VALUES(?)", (zpid,))
        conn.commit()

    conn.close()

