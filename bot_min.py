from __future__ import annotations
import os, json, logging, re, requests, time, html
from collections import defaultdict, Counter
from urllib.parse import urlparse

try:
    import phonenumbers
except ImportError:
    phonenumbers = None

try:
    from bs4 import BeautifulSoup
except ImportError:
    BeautifulSoup = None

import gspread
from googleapiclient.discovery import build
from google.oauth2.service_account import Credentials

CS_API_KEY = os.environ["CS_API_KEY"]
CS_CX = os.environ["CS_CX"]
GSHEET_ID = os.environ["GSHEET_ID"]
SC_JSON = json.loads(os.environ["GCP_SERVICE_ACCOUNT_JSON"])
SCOPES = ["https://www.googleapis.com/auth/spreadsheets"]

logging.basicConfig(level=os.getenv("LOGLEVEL", "DEBUG"),
                    format="%(asctime)s %(levelname)s: %(message)s")
LOG = logging.getLogger("bot")

SHORT_RE = re.compile(r"\bshort\s+sale\b", re.I)
BAD_RE = re.compile(r"approved|negotiator|settlement fee|fee at closing", re.I)

def is_short_sale(txt: str) -> bool:
    return bool(SHORT_RE.search(txt)) and not BAD_RE.search(txt)

IMG_EXT  = (".png",".jpg",".jpeg",".gif",".svg",".webp")
PHONE_RE = re.compile(r"(?<!\d)(?:\+?1[\s\-\.]*)?\(?\d{3}\)?[\s\-\.]*\d{3}[\s\-\.]*\d{4}(?!\d)")
EMAIL_RE = re.compile(r"[A-Z0-9._%+-]+@[A-Z0-9.-]+\.[A-Z]{2,}", re.I)

def fmt_phone(raw: str) -> str:
    digits = re.sub(r"\D","",raw)
    if len(digits)==11 and digits.startswith("1"):
        digits = digits[1:]
    return f"{digits[:3]}-{digits[3:6]}-{digits[6:]}" if len(digits)==10 else ""

US_AREA_CODES = {  # truncated set
    '201','202','203','205','206','207','208','209','210','212','213','214','215','216','217','218','219',
    '220','224','225','227','228','229','231','234','239','240','248','251','252','253','254','256','260',
    '262','267','269','270','272','276','281','301','302','303','304','305','307','308','309','310','312',
    '313','314','315','316','317','318','319','320','321','323','325','327','330','331','332','334','336',
    '337','339','346','347','351','352','360','361','364','380','385','386','401','402','404','405','406',
    '407','408','409','410','412','413','414','415','417','419','423','424','425','430','432','434','435',
    '440','442','443','447','458','463','464','469','470','475','478','479','480','484','501','502','503',
    '504','505','507','508','509','510','512','513','515','516','517','518','520','530','531','534','539',
    '540','541','551','559','561','562','563','564','567','570','571','572','573','574','575','580','585',
    '586','601','602','603','605','606','607','608','609','610','612','614','615','616','617','618','619',
    '620','623','626','628','629','630','631','636','641','646','650','651','657','659','660','661','662',
    '667','669','678','680','681','682','701','702','703','704','706','707','708','712','713','714','715',
    '716','717','718','719','720','724','725','727','730','731','732','734','737','740','743','747','754',
    '757','760','762','763','764','765','769','770','771','772','773','774','775','779','781','785','786',
    '801','802','803','804','805','806','808','810','812','813','814','815','816','817','818','819','820',
    '828','830','831','832','838','839','840','843','845','847','848','850','854','856','857','858','859',
    '860','862','863','864','865','870','872','878','901','903','904','906','907','908','909','910','912',
    '913','914','915','916','917','918','919','920','925','928','929','930','931','934','936','937','938',
    '940','941','945','947','949','951','952','954','956','959','970','971','972','973','978','979','980',
    '984','985','986','989'
}

def plausible_us_number(p:str)->bool:
    return bool(re.fullmatch(r"\d{3}-\d{3}-\d{4}",p)) and p[:3] in US_AREA_CODES

def valid_phone(p:str)->bool:
    if phonenumbers:
        try:
            return phonenumbers.is_possible_number(phonenumbers.parse(p,"US"))
        except Exception:
            return False
    return plausible_us_number(p)

def ok_email(addr:str)->bool:
    return not addr.lower().endswith(IMG_EXT)

LABEL_TABLE = {
    "mobile":4,"cell":4,"direct":4,"text":4,"c:":4,"m:":4,
    "phone":2,"tel":2,"p:":2,
    "office":1,"main":1,"customer":1,"footer":1
}
LABEL_RE = re.compile(r"(" + "|".join(map(re.escape,LABEL_TABLE.keys())) + r")", re.I)

def proximity_scan(html_text:str)->dict[str,tuple[int,int]]:
    hits={}
    for m in PHONE_RE.finditer(html_text):
        phone=fmt_phone(m.group())
        if not valid_phone(phone):
            continue
        snippet=html_text[max(m.start()-80,0):min(m.end()+80,len(html_text))]
        lab=LABEL_RE.search(snippet)
        w=LABEL_TABLE.get(lab.group().lower(),0) if lab else 0
        if w<2:
            continue
        s=2+w
        best,tot=hits.get(phone,(0,0))
        hits[phone]=(max(best,w),tot+s)
    return hits

def extract_structured_contacts(html_text:str)->tuple[list[str],list[str]]:
    phones,mails=[],[]
    if not BeautifulSoup:
        return phones,mails
    soup=BeautifulSoup(html_text,"html.parser")
    for tag in soup.find_all("script",{"type":"application/ld+json"}):
        try:
            data=json.loads(tag.string or "")
        except Exception:
            continue
        if isinstance(data,list):
            data=data[0]
        if isinstance(data,dict):
            tel=data.get("telephone") or (data.get("contactPoint") or {}).get("telephone")
            mail=data.get("email") or (data.get("contactPoint") or {}).get("email")
            if tel:
                phones.append(fmt_phone(tel))
            if mail:
                mails.append(mail)
    for a in soup.select('a[href^="tel:"]'):
        phones.append(fmt_phone(a["href"].split("tel:")[-1]))
    for a in soup.select('a[href^="mailto:"]'):
        mails.append(a["href"].split("mailto:")[-1])
    return phones,mails

creds=Credentials.from_service_account_info(SC_JSON,scopes=SCOPES)
sheets_service=build("sheets","v4",credentials=creds,cache_discovery=False)
gc=gspread.authorize(creds)
ws=gc.open_by_key(GSHEET_ID).sheet1

def phone_exists(phone:str)->bool:
    try:
        return phone in ws.col_values(3)
    except Exception:
        return False

def append_row(row:list[str]):
    sheets_service.spreadsheets().values().append(
        spreadsheetId=GSHEET_ID,
        range="Sheet1!A1",
        valueInputOption="RAW",
        body={"values":[row]}
    ).execute()

def fetch_text(url:str)->str|None:
    for target in (url,f"https://r.jina.ai/http://{url}"):
        try:
            r=requests.get(target,timeout=10,headers={"User-Agent":"Mozilla/5.0"})
            if r.status_code!=200 and r.status_code not in (403,429,999):
                continue
            txt=r.text
            if "unusual traffic" in txt[:600].lower():
                continue
            return txt
        except Exception:
            continue
    return None

AGENT_SITES=[
    "realtor.com","zillow.com","redfin.com","homesnap.com","kw.com","remax.com",
    "coldwellbanker.com","compass.com","exprealty.com","bhhs.com","c21.com",
    "realtyonegroup.com","mlsmatrix.com","mlslistings.com","har.com","brightmlshomes.com"
]
DOMAIN_CLAUSE=" OR ".join(f"site:{d}" for d in AGENT_SITES)

def build_queries(agent:str,state:str)->list[str]:
    return [
        f'"{agent}" {state} ("mobile" OR "cell" OR "direct") phone email ({DOMAIN_CLAUSE})',
        f'"{agent}" {state} phone email ({DOMAIN_CLAUSE})',
        f'"{agent}" {state} contact ({DOMAIN_CLAUSE})'
    ]

AREA_BY_STATE={
    "FL":["305","321","352","386","407","561","727","754","772","786","813","850","863","904","941","954"],
    "GA":["404","470","478","678","706","770","912"],
    
"TX":["210","214","254","281","325","346","361","409","430","432","469","512","682","713","737","806","817","830","832","903","915","936","940","956","972","979","985"],
    "OK":["405","539","580","918"],
    
"CA":["209","213","310","323","408","415","424","442","510","530","559","562","619","626","650","657","661","669","707","714","747","760","805","818","820","831","858","909","916","925","949","951"]
}

agent_cache:dict[str,tuple[str,str]]={}

def realtor_fallback(agent:str,state:str)->tuple[str,str]:
    first,*last=agent.split()
    if not last:
        return "",""
    url=f"https://www.realtor.com/realestateagents/{'-'.join([first.lower()]+last).lower()}_{state.lower()}"
    html_txt=fetch_text(url)
    if not html_txt:
        return "",""
    phones,mails=extract_structured_contacts(html_txt)
    phone=next((p for p in phones if valid_phone(p)), "")
    email=mails[0] if mails else ""
    return phone,email

def google_lookup(agent:str,state:str)->tuple[str,str]:
    key=f"{agent}|{state}"
    if key in agent_cache:
        return agent_cache[key]
    candidate_phone:dict[str,tuple[int,int]]={}
    candidate_email:dict[str,int]=defaultdict(int)
    for q in build_queries(agent,state):
        time.sleep(0.25)
        try:
            items=requests.get("https://www.googleapis.com/customsearch/v1",
                params={"key":CS_API_KEY,"cx":CS_CX,"q":q,"num":10},timeout=10
            ).json().get("items",[])
        except Exception:
            continue
        for it in items:
            meta=it.get("pagemap",{})
            tel=meta.get("contactpoint",[{}])[0].get("telephone")
            mail=meta.get("contactpoint",[{}])[0].get("email")
            if tel:
                p=fmt_phone(tel)
                if valid_phone(p):
                    bw,ts=candidate_phone.get(p,(0,0))
                    candidate_phone[p]=(4,max(ts,0)+6)
            if mail and ok_email(mail):
                candidate_email[mail]+=3
        for it in items:
            url=it.get("link","")
            html_txt=fetch_text(url)
            if not html_txt or agent.lower() not in html_txt.lower():
                continue
            p_list,m_list=extract_structured_contacts(html_txt)
            for p in p_list:
                p_fmt=fmt_phone(p)
                if valid_phone(p_fmt):
                    bw,ts=candidate_phone.get(p_fmt,(0,0))
                    candidate_phone[p_fmt]=(4,max(ts,0)+6)
            for m in m_list:
                if ok_email(m):
                    candidate_email[m]+=3
            low=html.unescape(html_txt.lower())
            prox=proximity_scan(low)
            for p,(bw,s) in prox.items():
                pbw,pts=candidate_phone.get(p,(0,0))
                candidate_phone[p]=(max(pbw,bw),pts+s)
            for m in EMAIL_RE.findall(low):
                if ok_email(m) and agent.split()[-1].lower() in m.lower():
                    candidate_email[m]+=1
            if candidate_phone and candidate_email:
                break
        if candidate_phone and candidate_email:
            break
    if not candidate_phone:
        fb_p,fb_e=realtor_fallback(agent,state)
        if fb_p:
            candidate_phone[fb_p]=(3,3)
        if fb_e:
            candidate_email[fb_e]=2
    phone=""
    if candidate_phone:
        phone=max(candidate_phone.items(),key=lambda kv:(kv[1][0],kv[1][1]))[0]
    if phone and phone[:3] not in AREA_BY_STATE.get(state.upper(),[]):
        LOG.debug("area mismatch reject %s",phone)
        phone=""
    if not phone and candidate_phone:
        phone=list(candidate_phone.keys())[0]
    email=max(candidate_email,key=candidate_email.get) if candidate_email else ""
    agent_cache[key]=(phone,email)
    return phone,email

creds=Credentials.from_service_account_info(SC_JSON,scopes=SCOPES)
sheets_service=build("sheets","v4",credentials=creds,cache_discovery=False)
gc=gspread.authorize(creds)
ws=gc.open_by_key(GSHEET_ID).sheet1

def process_rows(rows:list[dict]):
    for r in rows:
        if not is_short_sale(r.get("description","")):
            continue
        agent=r.get("agentName","").strip()
        if not agent:
            continue
        phone,email=google_lookup(agent,r.get("state",""))
        phone=fmt_phone(phone)
        if phone and phone_exists(phone):
            continue
        first,*last=agent.split()
        append_row([
            first," ".join(last),phone,email,
            r.get("street",""),r.get("city",""),r.get("state","")
        ])

