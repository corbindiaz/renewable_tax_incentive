#!/usr/bin/env python3
import re
import time
import os
import json
import zipfile
import requests
import pandas as pd
import numpy as np
from io import BytesIO
from functools import reduce
from bs4 import BeautifulSoup
from urllib.parse import urljoin
from typing import List, Optional, Union
from playwright.sync_api import sync_playwright, TimeoutError as PWTimeout

# ─── STATE SCRAPERS ──────────────────────────────────────────────────────────

def extract_history_marker_ark(text: str):
    u4 = re.findall(r'\u2002(\d{4})', text)
    if u4:
        return int(u4[-1])
    parts = re.split(r'History', text, maxsplit=1, flags=re.IGNORECASE)
    history = parts[1] if len(parts) > 1 else text
    m = re.search(r'Acts\s+(\d{4})', history)
    if m:
        return int(m.group(1))
    m2 = re.search(r'Source:\s*L\.\s*(\d{4})', history)
    if m2:
        return int(m2.group(1))
    u2 = re.findall(r'\u2002(\d{2})', text)
    if u2:
        return int(u2[-1])
    m3 = re.search(r'Acts\s+(\d{2})', history)
    if m3:
        return int(m3.group(1))
    m4 = re.search(r'Source:\s*L\.\s*(\d{2})', history)
    if m4:
        return int(m4.group(1))
    if re.search(r'\[Reserved\.\]', history, re.IGNORECASE):
        return "Reserved"
    if re.search(r'\[Repealed\.\]', history, re.IGNORECASE):
        return "Repealed"
    return None

def trim_to_body_ark(text: str) -> str:
    marker = "\n\n\n"
    idx = text.find(marker)
    if idx != -1:
        return text[idx+len(marker):]
    parts = re.split(r'(?:\r?\n){3,}', text, maxsplit=1)
    return parts[1] if len(parts)>1 else text

def scrape_arkansas_df() -> pd.DataFrame:
    texts = []
    start = time.time()
    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True)
        page = browser.new_page()
        page.goto(
            "https://advance.lexis.com/container?config=00JAA3ZTU0NTIzYy0zZDEyLTRhYmQtYmRmMS1iMWIxNDgxYWMxZTQK"
            "AFBvZENhdGFsb2cubRW4ifTiwi5vLw6cI1uX&crid=8efec3df-f9c3-48dd-af22-2d09db950145", 
            wait_until="networkidle"
        )
        try:
            page.wait_for_selector("input#btnagreeterms", timeout=5_000)
            page.click("input#btnagreeterms")
        except PWTimeout:
            pass

        page.fill("textarea#searchTerms", "energy")
        page.press("textarea#searchTerms","Enter")
        page.wait_for_load_state("networkidle")

        while True:
            if time.time()-start > 600:
                break
            page.wait_for_selector("li.usview", timeout=20_000)
            hits = page.locator("li.usview")
            n = hits.count()
            for i in range(n):
                if time.time()-start>600:
                    break
                hits.nth(i).locator("p.min.vis a").first.click()
                page.wait_for_load_state("networkidle")
                page.wait_for_selector("section#document", timeout=10_000)
                texts.append(page.inner_text("section#document"))
                page.go_back()
                page.wait_for_load_state("networkidle")
            time.sleep(1)
            try:
                nxt = page.locator("nav.pagination >> a:has-text('Next')")
                if nxt.count() and nxt.first.is_visible():
                    nxt.first.click()
                    page.wait_for_load_state("networkidle")
                    continue
            except PWTimeout:
                pass
            break
        browser.close()

    seen = set(); uniq = []
    for t in texts:
        if t not in seen:
            seen.add(t); uniq.append(t)
    df = pd.DataFrame(uniq, columns=["text"])
    df["year"] = df["text"].apply(extract_history_marker_ark)
    df["text"] = df["text"].apply(trim_to_body_ark)
    df = df[df["year"].notnull()].copy()
    df["year"] = df["year"].apply(lambda y: (1900+y) if isinstance(y,int) and y<100 else y)
    df["state"] = "Arkansas"
    return df[["year","state","text"]]

def extract_history_marker_ga(text: str):
    m = re.search(r'Ga\.\s*L\.\s*(\d{4})', text)
    if m:
        return int(m.group(1))
    if re.search(r'\[Reserved\.\]', text, re.IGNORECASE):
        return "Reserved"
    if re.search(r'\[Repealed\.\]', text, re.IGNORECASE):
        return "Repealed"
    return None

def trim_to_body_ga(text: str) -> str:
    marker="\n\n\n"; idx=text.find(marker)
    if idx!=-1:
        return text[idx+len(marker):]
    parts=re.split(r'(?:\r?\n){3,}', text, maxsplit=1)
    return parts[1] if len(parts)>1 else text

def scrape_georgia_df() -> pd.DataFrame:
    texts=[]; start=time.time()
    with sync_playwright() as p:
        browser=p.chromium.launch(headless=True)
        page=browser.new_page()
        page.goto(
            "https://advance.lexis.com/container?config=00JAAzZDgzNzU2ZC05MDA0LTRmMDItYjkzMS0xOGY3MjE3OWNlODIK"
            "AFBvZENhdGFsb2fcIFfJnJ2IC8XZi1AYM4Ne&crid=d1ef0e4a-f560-4a3f-bca1-09d66269998b",
            wait_until="networkidle"
        )
        try:
            page.wait_for_selector("input#btnagreeterms", timeout=5_000)
            page.click("input#btnagreeterms")
        except PWTimeout:
            pass

        page.fill("textarea#searchTerms","energy")
        page.press("textarea#searchTerms","Enter")
        page.wait_for_load_state("networkidle")

        while True:
            if time.time()-start>600:
                break
            page.wait_for_selector("li.usview", timeout=20_000)
            hits=page.locator("li.usview"); n=hits.count()
            for i in range(n):
                if time.time()-start>600:
                    break
                try:
                    hits.nth(i).locator("p.min.vis a").first.click()
                    page.wait_for_load_state("networkidle")
                    page.wait_for_selector("section#document",timeout=10_000)
                    texts.append(page.inner_text("section#document"))
                except:
                    texts.append("")
                finally:
                    try:
                        page.go_back(); page.wait_for_load_state("networkidle")
                    except: pass
            time.sleep(1)
            try:
                nxt=page.locator("nav.pagination >> a[data-action='nextpage']").first
                if nxt.count() and nxt.is_visible():
                    nxt.click(); page.wait_for_load_state("networkidle"); continue
            except PWTimeout:
                pass
            break
        browser.close()

    seen=set(); uniq=[]
    for t in texts:
        if t not in seen:
            seen.add(t); uniq.append(t)
    df=pd.DataFrame(uniq,columns=["text"])
    df["year"]=df["text"].apply(extract_history_marker_ga)
    df=df[df["year"].notnull()].copy()
    df["text"]=df["text"].apply(trim_to_body_ga)
    df["year"]=df["year"].apply(lambda y:(1900+y) if isinstance(y,int) and y<100 else y)
    df["state"]="Georgia"
    return df[["year","state","text"]]

def extract_history_marker_co(text:str):
    return extract_history_marker_ark(text)

def trim_to_body_co(text:str)->str:
    return trim_to_body_ark(text)

def scrape_colorado_df()->pd.DataFrame:
    texts=[]; start=time.time()
    with sync_playwright() as p:
        browser=p.chromium.launch(headless=True)
        page=browser.new_page()
        page.goto(
            "https://advance.lexis.com/container?config=00JAA3ZTU0NTIzYy0zZDEyLTRhYmQtYmRmMS1iMWIxNDgxYWMxZTQK"
            "AFBvZENhdGFsb2cubRW4ifTiwi5vLw6cI1uX&crid=71f400f1-686d-4c50-8ecc-7711eca7c5a8",
            wait_until="networkidle"
        )
        try:
            page.wait_for_selector("input#btnagreeterms",timeout=5_000)
            page.click("input#btnagreeterms")
        except PWTimeout:
            pass

        page.fill("textarea#searchTerms","energy")
        page.press("textarea#searchTerms","Enter")
        page.wait_for_load_state("networkidle")

        while True:
            if time.time()-start>600:
                break
            page.wait_for_selector("li.usview",timeout=20_000)
            hits=page.locator("li.usview"); n=hits.count()
            for i in range(n):
                if time.time()-start>600:
                    break
                hits.nth(i).locator("p.min.vis a").first.click()
                page.wait_for_load_state("networkidle")
                page.wait_for_selector("section#document",timeout=10_000)
                texts.append(page.inner_text("section#document"))
                page.go_back(); page.wait_for_load_state("networkidle")
            time.sleep(1)
            try:
                nxt=page.locator("nav.pagination >> a:has-text('Next')")
                if nxt.count() and nxt.first.is_visible():
                    nxt.first.click(); page.wait_for_load_state("networkidle"); continue
            except PWTimeout:
                pass
            break
        browser.close()

    seen=set(); uniq=[]
    for t in texts:
        if t not in seen:
            seen.add(t); uniq.append(t)
    df=pd.DataFrame(uniq,columns=["text"])
    df["year"]=df["text"].apply(extract_history_marker_co)
    df["text"]=df["text"].apply(trim_to_body_co)
    df=df[df["year"].notnull()].copy()
    df["year"]=df["year"].apply(lambda y:(1900+y) if isinstance(y,int) and y<100 else y)
    df["state"]="Colorado"
    return df[["year","state","text"]]

def extract_last_approval_year_de(text)->Optional[int]:
    pattern = r"""(?:Approved|Passed(?:\s+at\s+[^,]+)?),?\s*[A-Za-z]+\s+\d{1,2}(?:[.,]\s*)(?:A\.\s*D\.\s*)?(\d{4})"""
    m = list(re.finditer(pattern,text,flags=re.IGNORECASE|re.VERBOSE))
    return int(m[-1].group(1)) if m else None

def scrape_delaware_df()->pd.DataFrame:
    section_urls=[]; start=time.time()
    with sync_playwright() as p:
        b=p.chromium.launch(headless=True); pg=b.new_page(viewport={"width":1280,"height":800})
        pg.goto("https://delcode.delaware.gov/",wait_until="networkidle")
        pg.fill("input#srch-term","energy"); pg.click("button[onclick='submitSearch()']")
        pg.wait_for_selector("#SearchResultsGrid tbody tr.k-master-row",timeout=15_000)
        while True:
            if time.time()-start>600: break
            rows=pg.locator("#SearchResultsGrid tbody tr.k-master-row"); cnt=rows.count()
            for i in range(cnt):
                if time.time()-start>600: break
                cells=rows.nth(i).locator("td[role='gridcell']")
                if cells.count()>=4:
                    link=cells.nth(3).locator("a").first
                    href=link.get_attribute("href"); txt=link.inner_text().strip()
                    if href:
                        full=href if href.startswith("http") else "https://delcode.delaware.gov"+href
                        section_urls.append((full,txt))
            if pg.locator("li.k-pager-nav.k-state-disabled").count(): break
            pg.click("a.k-link.k-pager-nav[title='Go to the next page']")
            pg.wait_for_selector("#SearchResultsGrid tbody tr.k-master-row",timeout=15_000)
        b.close()

    chap_urls=[]; start=time.time()
    for url,sec in section_urls:
        if time.time()-start>600: break
        with sync_playwright() as p:
            b=p.chromium.launch(headless=True); pg=b.new_page()
            pg.goto(url,wait_until="domcontentloaded")
            js=f"""() => {{
              const head=document.querySelector("div.SectionHead[id='{sec}']");
              if(!head)return[];
              const s=head.closest("div.Section");
              return Array.from(s.querySelectorAll("a")).map(a=>a.href);
            }}"""
            raw=pg.evaluate(js); b.close()
        for h in raw:
            if h not in chap_urls:
                chap_urls.append(h)

    docs=[]; years=[]
    for u in chap_urls:
        if time.time()-start>600: break
        with sync_playwright() as p:
            b=p.chromium.launch(headless=True); pg=b.new_page()
            pg.goto(u,wait_until="domcontentloaded")
            pg.wait_for_selector("div#chapterBody",timeout=10_000)
            txt=pg.inner_text("div#chapterBody"); b.close()
        docs.append(txt); years.append(extract_last_approval_year_de(txt))

    df=pd.DataFrame({"year":years,"text":docs}).dropna(subset=["year"])
    df["state"]="Delaware"
    return df[["year","state","text"]]

def extract_original_enactment_year_fl(h:str)->Optional[int]:
    parts=re.split(r'\n\n—\s*History\s*—\n\n',h,flags=re.IGNORECASE)
    if len(parts)<2: return None
    m=re.search(r'ch\.\s*(\d{2,4})-',parts[1])
    if not m: return None
    raw=int(m.group(1))
    return (1900+raw) if raw<100 and raw>=50 else (2000+raw) if raw<50 else raw

def scrape_florida_df()->pd.DataFrame:
    links=[]; start=time.time()
    with sync_playwright() as p:
        b=p.firefox.launch(headless=True); pg=b.new_page()
        pg.goto("https://www.flsenate.gov/laws/statutes",wait_until="domcontentloaded")
        pg.fill("input#filteredData_StatuteSearchQuery","energy")
        pg.click("input[name='StatutesGoSubmit']")
        pg.wait_for_selector("table.tbl.width100 a",timeout=10_000)
        for a in pg.query_selector_all("table.tbl.width100 a"):
            href=a.get_attribute("href"); txt=a.inner_text().strip()
            if href:
                links.append((txt,f"https://www.flsenate.gov{href}"))
        b.close()

    docs=[]; years=[]
    for _,url in links:
        if time.time()-start>600: break
        with sync_playwright() as p:
            b=p.chromium.launch(headless=True); pg=b.new_page()
            pg.goto(url,wait_until="domcontentloaded")
            pg.wait_for_selector("span.SectionBody div.Paragraph",timeout=10_000)
            paras=pg.locator("span.SectionBody div.Paragraph").all_text_contents()
            body="\n\n".join(p.strip() for p in paras if p.strip())
            hl=pg.locator("div.History span.HistoryText")
            hist=hl.count() and hl.inner_text().strip() or ""
            full=(f"{body}\n\n— History —\n\n{hist}" if hist else body)
            b.close()
        docs.append(full); years.append(extract_original_enactment_year_fl(full))

    df=pd.DataFrame({"year":years,"text":docs}).dropna(subset=["year"])
    df["state"]="Florida"
    return df[["year","state","text"]]

# ─── NON‑STATE DATA ──────────────────────────────────────────────────────────

def law_data() -> pd.DataFrame:
    # NREL CSV
    csv_url = ("https://developer.nrel.gov/api/transportation-incentives-laws/v1.csv?"
               "api_key=GAmcMbhWclW5qULHxvWQWtUw52EsehwTPtfu4cz8&expired=false"
               "&incentive_type=GNT%2CTAX%2CLOANS%2CRBATE%2CEMREC%2CTOU%2COTHER"
               "&law_type=INC%2CPROG%2CLAWREG%2CSTATEINC"
               "&regulation_type=REQ%2CDREST%2CREGIS%2CEVFEE%2CFUEL%2CSTD%2CRFS%2CAIRQEMISSIONS%2CCCEINIT%2CUTILITY%2CBUILD%2CRTC%2COTHER"
               "&technology=BIOD%2CETH%2CNG%2CLPG%2CHY%2CELEC%2CPHEV%2CHEV%2CNEVS%2CRD%2CAFTMKTCONV%2CEFFEC%2CIR%2CAUTONOMOUS%2COTHER"
               "&user_type=FLEET%2CGOV%2CTRIBAL%2CIND%2CSTATION%2CAFP%2CPURCH%2CMAN%2CMUD%2CTRANS%2COTHER")
    nrel = pd.read_csv(csv_url)
    nrel["state"] = nrel["State"].str.upper()
    nrel["year"] = pd.to_datetime(nrel["Status Date"], errors="coerce").dt.year.astype("Int64")

    # 48C
    url48 = ("http://edx.netl.doe.gov/dataset/22944d5d-d063-4890-a995-064bc59b5a78/"
             "resource/3d01f2d6-1c1c-498d-8db2-3a51aa3c07f2/download")
    r = requests.get(url48, headers={"User-Agent":"Mozilla/5.0"})
    with zipfile.ZipFile(BytesIO(r.content)) as z:
        for f in z.namelist():
            if f.endswith(".csv"):
                df48 = pd.read_csv(z.open(f))
                # assume State_Name, date_last_ present
                df48["state"] = df48["State_Name"]
                df48["year"] = pd.to_datetime(df48["date_last_"], errors="coerce").dt.year.astype("Int64")
    # merge
    merged = pd.merge(df48, nrel, on=["state","year"], how="outer", suffixes=("_48c","_nrel"))
    merged["metadata"] = merged.drop(columns=["state","year"]).astype(str).agg("|".join,axis=1)
    return merged[["year","state","metadata"]]

def community_data() -> pd.DataFrame:
    dfs=[]
    endpoints = [
      ("FFE", "https://arcgis.netl.doe.gov/server/rest/services/Hosted/2024_MSAs_NonMSAs_that_only_meet_the_FFE_Threshold/FeatureServer/0/query"),
      ("EC",  "https://arcgis.netl.doe.gov/server/rest/services/Hosted/2024_MSAs_NonMSAs_that_are_Energy_Communities/FeatureServer/0/query"),
      ("CPP", "https://arcgis.netl.doe.gov/server/rest/services/Hosted/US_Power_Plants/FeatureServer/0/query?where=LOWER(plant_status)='closed'"),
      ("48CM","https://arcgis.netl.doe.gov/server/rest/services/Hosted/ManFacNAICS3133_Emissions/FeatureServer/0/query?where=LOWER(status_48c)='eligible for 48c tax credit as a designated energy community'"),
      ("Coal","https://arcgis.netl.doe.gov/server/rest/services/Hosted/US_generators_coal/FeatureServer/0/query?where=((f_860m_retirementyear<=2030)AND(f_860m_nameplatecapacity_mw_>=53))")
    ]
    for tag,url in endpoints:
        resp = requests.get(url, params={"where":"1=1","outFields":"*","f":"json","resultRecordCount":8000}).json()
        feats = resp.get("features",[])
        if not feats:
            continue
        df = pd.json_normalize([f["attributes"] for f in feats])
        # pick first state-like & year-like column
        state_col = next((c for c in df if "state" in c.lower()), None)
        year_col  = next((c for c in df if "year"  in c.lower()), None)
        df["state"] = df[state_col].astype(str) if state_col else "Unknown"
        df["year"]  = pd.to_numeric(df[year_col], errors="coerce").astype("Int64") if year_col else pd.NA
        df["metadata"] = df.drop(columns=["state","year"], errors="ignore").to_dict(orient="records")
        dfs.append(df[["year","state","metadata"]])
    return pd.concat(dfs,ignore_index=True) if dfs else pd.DataFrame(columns=["year","state","metadata"])

def energy_data() -> pd.DataFrame:
    # ATB
    atb_folder = os.path.join("Energy Data","ATB")
    years = [d for d in os.listdir(atb_folder) if d.isdigit()]
    latest = max(years)
    files = [f for f in os.listdir(os.path.join(atb_folder,latest)) if f.endswith(".csv")]
    atb_df = pd.read_csv(os.path.join(atb_folder,latest,files[0]))
    atb_df["state"]="US"
    atb_df["metadata"] = atb_df.drop(columns=[atb_df.columns[0]]).to_dict(orient="records")
    atb_df["year"] = pd.to_numeric(atb_df[atb_df.columns[0]], errors="coerce").astype("Int64")
    atb_meta = atb_df[["year","state","metadata"]]

    # RECS
    recs_folder = os.path.join("Energy Data","RECS")
    recs_year = max(d for d in os.listdir(recs_folder) if d.isdigit())
    recs_path = os.path.join(recs_folder,recs_year,
                              next(f for f in os.listdir(os.path.join(recs_folder,recs_year)) if f.lower().endswith(".csv")))
    recs_df = pd.read_csv(recs_path, low_memory=False)
    recs_df = recs_df.rename(columns={"state_postal":"state"})
    recs_df["year"] = int(recs_year)
    recs_df["metadata"] = recs_df.drop(columns=["state"]).to_dict(orient="records")
    recs_meta = recs_df[["year","state","metadata"]]

    # SEDS
    seds_folder = os.path.join("Energy Data","SEDS")
    seds_year = max(d for d in os.listdir(seds_folder) if d.isdigit())
    seds_path = next(os.path.join(seds_folder,seds_year,f)
                     for f in os.listdir(os.path.join(seds_folder,seds_year)) if "complete" in f.lower())
    seds_df = pd.read_csv(seds_path, low_memory=False)
    wide = seds_df.pivot_table(index=["Year","StateCode"], columns="MSN", values="Data").reset_index()
    wide = wide.rename(columns={"Year":"year","StateCode":"state"})
    wide["metadata"] = wide.drop(columns=["year","state"]).to_dict(orient="records")
    seds_meta = wide[["year","state","metadata"]]

    # RMI
    rmi_folder = os.path.join("Energy Data","RMI")
    parts = []
    for f in os.listdir(rmi_folder):
        if f.endswith(".csv"):
            df = pd.read_csv(os.path.join(rmi_folder,f), dtype=str)
            df["year"] = pd.to_numeric(df["year"], errors="coerce").astype("Int64")
            sc = next((c for c in df if "state" in c.lower()), None)
            df["state"] = df[sc] if sc else "NA"
            df["metadata"] = df.drop(columns=["year","state"]).to_dict(orient="records")
            parts.append(df[["year","state","metadata"]])
    rmi_meta = pd.concat(parts,ignore_index=True) if parts else pd.DataFrame(columns=["year","state","metadata"])

    return pd.concat([atb_meta, recs_meta, seds_meta, rmi_meta], ignore_index=True)

def main():
    # scrape states
    ark = scrape_arkansas_df()
    ga  = scrape_georgia_df()
    co  = scrape_colorado_df()
    de  = scrape_delaware_df()
    fl  = scrape_florida_df()
    state_df = pd.concat([ark, ga, co, de, fl], ignore_index=True)
    state_df.to_csv("combined_states.csv", index=False)
    state_df.to_excel("combined_states.xlsx", index=False)

    # non‑state data
    law_df   = law_data()
    comm_df  = community_data()
    energy_df= energy_data()

    # merge all
    merged = state_df.merge(law_df,   on=["year","state"], how="outer") \
                     .merge(comm_df,  on=["year","state"], how="outer") \
                     .merge(energy_df,on=["year","state"], how="outer")

    merged.to_csv("final_merged_df.csv", index=False)
    merged.to_excel("final_merged_df.xlsx", index=False)
    print(f"✅ Done: {len(merged)} total records saved to final_merged_df.*")

if __name__ == "__main__":
    main()
