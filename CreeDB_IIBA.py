# -*- coding: utf-8 -*-
"""
Script de migration IIBA Cameroun
Transforme "IIBA DB notes Mars25.xlsx" en "IIBA_import_multisheets_auto.xlsx"
avec plusieurs onglets (Contacts, Interactions, Evénements, Participations, Paiements, Certifications).
"""

import pandas as pd
import re
from pathlib import Path
from datetime import datetime

# -----------------------
# 1. Charger le fichier source
# -----------------------
INPUT_FILE = "IIBA DB notes Mars25.xlsx"
OUTPUT_FILE = "IIBA_import_multisheets_auto.xlsx"

df_raw = pd.read_excel(INPUT_FILE, sheet_name=None)  # charge toutes les feuilles

# Fusionne toutes les feuilles en un seul DataFrame brut
df_all = pd.concat(df_raw.values(), ignore_index=True)

# -----------------------
# 2. Nettoyage et préparation Contacts
# -----------------------
contacts = []

for idx, row in df_all.iterrows():
    contact = {
        "ID": f"C{1000+idx}",  # ID unique
        "Nom": str(row.get("Nom", "")).strip(),
        "Prénom": str(row.get("Prénom", "")).strip(),
        "Email": str(row.get("Email", "")).strip().lower(),
        "Téléphone": str(row.get("Téléphone", "")),
        "Entreprise": str(row.get("Entreprise", "")),
        "Ville": str(row.get("Ville", "")),
        "Pays": str(row.get("Pays", "")) if "Pays" in row else "Cameroun",
        "Source": str(row.get("Source", "Import Excel")),
        "Statut": "Prospect",
        "Date_Création": datetime.today().strftime("%Y-%m-%d")
    }
    contacts.append(contact)

df_contacts = pd.DataFrame(contacts).drop_duplicates(subset=["Email", "Téléphone"])
# -----------------------
# 3. Fonctions utilitaires d'inférence
# -----------------------

import numpy as np

def _norm_str(x: str) -> str:
    return re.sub(r"\s+", " ", str(x or "")).strip()

def _any_text(row: pd.Series) -> str:
    """
    Concatène les champs susceptibles de contenir des infos d'événements, montants, interactions.
    Adapte les noms si besoin (ajoute d'autres noms de colonnes présents dans ton classeur).
    """
    fields = [
        "Notes", "Commentaires", "Commentaire", "Historique", "Activités", "Activity",
        "Détails", "Details", "Observations", "Remarques", "Remark"
    ]
    bits = []
    for f in fields:
        if f in row and pd.notna(row[f]) and str(row[f]).strip():
            bits.append(str(row[f]))
    # fallback : email/société peuvent parfois contenir des mentions
    if not bits:
        for f in ["Email", "Entreprise", "Société"]:
            if f in row and pd.notna(row[f]) and str(row[f]).strip():
                bits.append(str(row[f]))
    return "\n".join(bits)

def infer_event_type(text: str, name_hint: str = "") -> str:
    t = text.lower() + " " + name_hint.lower()
    if any(k in t for k in ["webinar", "webinaire", "zoom", "teams", "online", "virtuel"]):
        return "Webinaire"
    if any(k in t for k in ["groupe d'etude", "groupe d’étude", "study group"]):
        return "Groupe d'étude"
    if "ba meet" in t or "meet up" in t or "meetup" in t:
        return "BA MEET UP"
    if any(k in t for k in ["certif", "ecba", "ccba", "cbap", "pba"]):
        return "Certification"
    if any(k in t for k in ["formation", "atelier", "workshop", "bootcamp"]):
        return "Formation"
    if any(k in t for k in ["conf", "conference", "conférence", "seminaire", "séminaire"]):
        return "Conférence"
    return "Événement"

def infer_event_venue(text: str) -> str:
    t = text.lower()
    if any(k in t for k in ["zoom", "teams", "meet", "online", "virtuel", "en ligne"]):
        return "Zoom"
    if any(k in t for k in ["douala", "yaounde", "yaoundé", "salle", "presentiel", "présentiel", "limbe", "bafoussam", "garoua"]):
        return "Présentiel"
    return "Hybride"

def infer_event_year(text: str) -> str | None:
    m = re.search(r"(20\d{2})", text)
    return m.group(1) if m else None

def extract_amounts_fcfa(text: str) -> list[int]:
    """
    Extrait des montants (FCFA) du texte, ex : 50 000, 150000, 200.000, etc.
    """
    amounts = []
    for m in re.finditer(r"(\d[\d\s\.\,]{2,})\s*(?:fcfa|xaf|cfa|f)?", text, flags=re.I):
        raw = re.sub(r"[^\d]", "", m.group(1))
        if raw.isdigit():
            amounts.append(int(raw))
    return amounts

def looks_unpaid(text: str) -> bool:
    return bool(re.search(r"impay", text, re.I))

# -----------------------
# 4. Génération : Événements, Participations, Interactions, Paiements
# -----------------------

# Schémas cibles des onglets (compatibles Admin → Import multi-onglets)
EVENTS_COLS = ["ID_Événement","Nom_Événement","Type","Date","Durée_h","Lieu","Formateur","Objectif","Periode",
               "Cout_Salle","Cout_Formateur","Cout_Logistique","Cout_Pub","Cout_Autres","Cout_Total","Notes"]
PARTS_COLS  = ["ID_Participation","ID","ID_Événement","Rôle","Inscription","Arrivée","Temps_Present","Feedback","Note","Commentaire"]
INTER_COLS  = ["ID_Interaction","ID","Date","Canal","Objet","Résumé","Résultat","Prochaine_Action","Relance","Responsable"]
PAY_COLS    = ["ID_Paiement","ID","ID_Événement","Date_Paiement","Montant","Moyen","Statut","Référence","Notes","Relance"]

df_events = pd.DataFrame(columns=EVENTS_COLS)
df_parts  = pd.DataFrame(columns=PARTS_COLS)
df_inter  = pd.DataFrame(columns=INTER_COLS)
df_pay    = pd.DataFrame(columns=PAY_COLS)

# Map pour retrouver un contact ID par "clé dédupe" (email -> téléphone -> nom+prénom+société)
def contact_key(r: pd.Series) -> str:
    email = str(r.get("Email","")).strip().lower()
    tel   = re.sub(r"[^\d]", "", str(r.get("Téléphone","")))
    if email: return f"email:{email}"
    if tel:   return f"tel:{tel}"
    nom = str(r.get("Nom","")).strip().lower()
    prenom = str(r.get("Prénom","")).strip().lower()
    soc = str(r.get("Entreprise","") or r.get("Société","")).strip().lower()
    return f"nps:{nom}|{prenom}|{soc}"

id_by_key = {contact_key(r): r["ID"] for _, r in df_contacts.iterrows()}

def new_id(prefix: str, current: pd.Series) -> str:
    nums = current.dropna().astype(str).str.extract(r"(\d+)$")[0].dropna().astype(int)
    n = (nums.max() + 1) if not nums.empty else 1
    return f"{prefix}_{n:03d}"

# Mémoire pour dédupliquer les événements (Nom_Événement normalisé + Année)
event_index = {}   # key → EVT_ID
def event_key(name: str, year: str|None) -> str:
    name_norm = re.sub(r"\s+", " ", (name or "")).strip().lower()
    y = year or ""
    return f"{name_norm}::{y}"

for _, row in df_all.iterrows():
    # Texte “global” pour le contact
    text = _any_text(row)
    if not any([str(v).strip() for v in row.values]) and not text:
        continue  # ligne vide

    # Récupérer l'ID contact
    # On mappe d'abord sur le df_contacts dédupliqué
    ckey = contact_key(pd.Series({
        "Email": row.get("Email",""),
        "Téléphone": row.get("Téléphone",""),
        "Nom": row.get("Nom",""),
        "Prénom": row.get("Prénom",""),
        "Entreprise": row.get("Entreprise",""),
        "Société": row.get("Société",""),
    }))
    cid = id_by_key.get(ckey)
    if not cid:
        # Si le contact n'a pas été créé en Partie 1 (ex. colonnes manquantes),
        # on le crée sommairement ici.
        cid = new_id("CNT", df_contacts["ID"])
        df_contacts.loc[len(df_contacts)] = {
            "ID": cid,
            "Nom": _norm_str(row.get("Nom","")),
            "Prénom": _norm_str(row.get("Prénom","")),
            "Email": _norm_str(row.get("Email","")).lower(),
            "Téléphone": _norm_str(row.get("Téléphone","")),
            "Entreprise": _norm_str(row.get("Entreprise","") or row.get("Société","")),
            "Ville": _norm_str(row.get("Ville","")),
            "Pays": _norm_str(row.get("Pays","") or "Cameroun"),
            "Source": _norm_str(row.get("Source","Import Excel")),
            "Statut": "Prospect",
            "Date_Création": datetime.today().strftime("%Y-%m-%d")
        }
        id_by_key[ckey] = cid

    # 4.1 Interactions à partir des notes/commentaires (chaque puce/ligne courte)
    bullets = re.split(r"[\n•\-;•●◆►→]+", text) if text else []
    for b in [x.strip() for x in bullets if x and len(x.strip()) >= 6]:
        iid = new_id("INT", df_inter["ID_Interaction"])
        df_inter.loc[len(df_inter)] = [
            iid, cid, datetime.today().strftime("%Y-%m-%d"),
            "Notes", "Commentaire", b, "", "", "", "Système"
        ]

    # 4.2 Extraction d’événements (nom + année) et création des participations
    # Heuristique : mot(s) + année (2023–2026) OU “BA MEET UP …” / “Groupe d'étude …” / “Webinaire …”
    candidates = set()
    # pattern “mots … + 20xx”
    for m in re.finditer(r"([A-Za-zÀ-ÖØ-öø-ÿ'’\s]{3,})\s*(20\d{2})", text or "", flags=re.I):
        ev_name = _norm_str(m.group(1))
        ev_year = m.group(2)
        candidates.add((ev_name, ev_year))
    # marqueurs sans année → on tente de rattacher une année à défaut
    keywords = [
        r"BA\s*MEET\s*UP", r"Groupe d[’']?étude", r"Webinaire", r"Formation", r"Conférence", r"Workshop", r"Atelier"
    ]
    for kw in keywords:
        if re.search(kw, text or "", flags=re.I):
            # devine l’année
            y = infer_event_year(text) or str(datetime.today().year)
            # si on a dans la ligne des occurrences style “Groupe d’étude Azure”, on capture le libellé
            # sinon on prend le mot-clé comme nom
            label = re.search(kw + r"[^,\n;]*", text or "", flags=re.I)
            name = _norm_str(label.group(0)) if label else re.sub(r"\\", "", kw)
            candidates.add((name, y))

    for (name, year) in candidates:
        typ  = infer_event_type(text, name)
        lieu = infer_event_venue(text)
        k = event_key(name, year)
        if k in event_index:
            eid = event_index[k]
        else:
            eid = new_id("EVT", df_events["ID_Événement"])
            # Date approximative au milieu d’année si inconnue
            ev_date = f"{year}-06-15" if year else datetime.today().strftime("%Y-%m-%d")
            df_events.loc[len(df_events)] = [
                eid, f"{name} {year}".strip(), typ, ev_date, "2", lieu, "", "", "", 0,0,0,0,0,0,""
            ]
            event_index[k] = eid

        # Participation (par défaut : Participant)
        pid = new_id("PAR", df_parts["ID_Participation"])
        df_parts.loc[len(df_parts)] = [pid, cid, eid, "Participant", "", "", "", "", "", ""]

        # 4.3 Paiements : si on voit un montant → Réglé (Mobile Money), sinon “Non payé” si “impay” détecté
        amts = extract_amounts_fcfa(text or "")
        if amts:
            payid = new_id("PAY", df_pay["ID_Paiement"])
            df_pay.loc[len(df_pay)] = [
                payid, cid, eid, datetime.today().strftime("%Y-%m-%d"),
                int(amts[0]), "Mobile Money", "Réglé", "", "", ""
            ]
        elif looks_unpaid(text or ""):
            payid = new_id("PAY", df_pay["ID_Paiement"])
            df_pay.loc[len(df_pay)] = [
                payid, cid, eid, datetime.today().strftime("%Y-%m-%d"),
                0, "Mobile Money", "Non payé", "", "", ""
            ]

# Harmonisation des types (numériques) pour cohérence d’import
for col in ["Cout_Salle","Cout_Formateur","Cout_Logistique","Cout_Pub","Cout_Autres","Cout_Total","Durée_h"]:
    if col in df_events.columns:
        df_events[col] = pd.to_numeric(df_events[col], errors="coerce").fillna(0)

if "Montant" in df_pay.columns:
    df_pay["Montant"] = pd.to_numeric(df_pay["Montant"], errors="coerce").fillna(0)

# (Optionnel) tri par ID
for dfx, idcol in [(df_events,"ID_Événement"), (df_parts,"ID_Participation"), (df_inter,"ID_Interaction"), (df_pay,"ID_Paiement")]:
    if not dfx.empty and idcol in dfx.columns:
        dfx.sort_values(by=idcol, inplace=True)
# -----------------------
# 5. Normalisation finale + Règles de conversion/score
# -----------------------

# Colonnes cibles attendues par ton app (multi-onglets)
C_COLS_OUT = [
    "ID","Nom","Prénom","Genre","Titre","Société","Secteur","Email","Téléphone","LinkedIn",
    "Ville","Pays","Type","Source","Statut","Score_Engagement","Date_Creation","Notes","Top20"
]

# 5.1 Contacts → mapping vers colonnes cibles
def norm_bool(x):
    if isinstance(x, str):
        return x.strip().lower() in ("1","true","vrai","yes","oui")
    return bool(x)

dfc = df_contacts.copy()

# Colonnes manquantes
for c in ["Entreprise","Société","Genre","Titre","Secteur","LinkedIn","Source","Statut","Score_Engagement","Notes","Top20","Date_Création"]:
    if c not in dfc.columns:
        dfc[c] = ""

# Harmonisation Société / Date_Creation
dfc["Société"] = dfc["Société"].where(dfc["Société"].astype(str).str.strip()!="", dfc["Entreprise"])
dfc["Date_Creation"] = dfc["Date_Création"].replace("", pd.NaT)
dfc["Date_Creation"] = pd.to_datetime(dfc["Date_Creation"], errors="coerce").dt.date.fillna(pd.Timestamp.today().date())
dfc["Date_Creation"] = dfc["Date_Creation"].astype(str)

# 5.2 Agrégats utiles pour scoring / conversion
# Participations par contact
parts_by_contact = df_parts.groupby("ID")["ID_Participation"].count() if not df_parts.empty else pd.Series(dtype=int)
# Interactions par contact (totales)
inter_by_contact = df_inter.groupby("ID")["ID_Interaction"].count() if not df_inter.empty else pd.Series(dtype=int)
# Interactions récentes (≈ 90 jours) → toutes créées aujourd'hui par défaut ⇒ récentes
# Si tu veux calculer sur de vraies dates, adapte ici
recent_by_contact = inter_by_contact.copy()

# Paiements réglés et partiels par contact
if not df_pay.empty:
    df_pay["Montant"] = pd.to_numeric(df_pay["Montant"], errors="coerce").fillna(0)
    ca_regle_by_c = df_pay[df_pay["Statut"].str.lower()=="réglé"].groupby("ID")["Montant"].sum()
    has_partiel_by_c = df_pay[df_pay["Statut"].str.lower()=="partiel"].groupby("ID")["Montant"].count() > 0
else:
    ca_regle_by_c = pd.Series(dtype=float)
    has_partiel_by_c = pd.Series(dtype=bool)

# 5.3 Règles (cohérentes avec ton app.py)
INTERACTIONS_MIN_HOT = 3
PARTICIPATIONS_MIN_HOT = 1

def proba_conversion(row):
    inter_recent = int(recent_by_contact.get(row["ID"], 0))
    parts = int(parts_by_contact.get(row["ID"], 0))
    partiel = bool(has_partiel_by_c.get(row["ID"], False))
    if inter_recent >= INTERACTIONS_MIN_HOT and parts >= PARTICIPATIONS_MIN_HOT and partiel:
        return "Chaud"
    elif inter_recent >= 1 or parts >= 1:
        return "Tiède"
    return "Froid"

# Type par défaut = Prospect ; Membre si critères (ex. ≥1 paiement réglé ou ≥2 participations)
def infer_type(row):
    parts = int(parts_by_contact.get(row["ID"], 0))
    ca = float(ca_regle_by_c.get(row["ID"], 0.0))
    if ca > 0 or parts >= 2:
        return "Membre"
    return "Prospect"

# Statut par défaut : Actif s’il y a au moins 1 interaction/participation ; sinon À relancer
def infer_statut(row):
    inters = int(inter_by_contact.get(row["ID"], 0))
    parts = int(parts_by_contact.get(row["ID"], 0))
    if inters + parts > 0:
        return "Actif"
    return "À relancer"

# Score_Engagement simple (pondérations compatibles avec app)
W_INT = 1.0
W_PART = 1.0
W_PAY = 2.0
def score_engagement(row):
    inters = int(inter_by_contact.get(row["ID"], 0))
    parts = int(parts_by_contact.get(row["ID"], 0))
    ca_regles = float(ca_regle_by_c.get(row["ID"], 0.0))
    return round(W_INT*inters + W_PART*parts + W_PAY*(1 if ca_regles>0 else 0), 2)

# Application des règles
dfc["Type"] = dfc.apply(infer_type, axis=1)
dfc["Statut"] = dfc.apply(infer_statut, axis=1)
dfc["Score_Engagement"] = dfc.apply(score_engagement, axis=1)

# Top20 (marquage simple via liste d’entreprises cibles)
ENTREPRISES_CIBLES = {"dangote","mupeci","salam","sunu iard","eneo","pad","pak"}
def is_top20(s):
    s = str(s or "").strip().lower()
    return any(k in s for k in ENTREPRISES_CIBLES)
dfc["Top20"] = dfc["Société"].map(is_top20).astype(bool)

# Ajout Proba (facultatif pour vue)
dfc["Proba_conversion"] = dfc.apply(proba_conversion, axis=1)

# 5.4 Normalisation finale des colonnes Contacts
for c in C_COLS_OUT:
    if c not in dfc.columns:
        dfc[c] = ""

df_contacts_out = dfc[C_COLS_OUT].copy()

# -----------------------
# 6. Certifications (facultatif : vide si non déductible)
# -----------------------
CERT_COLS_OUT = ["ID_Certif","ID","Type_Certif","Date_Examen","Résultat","Score","Date_Obtention","Validité","Renouvellement","Notes"]
df_cert = pd.DataFrame(columns=CERT_COLS_OUT)  # si tu as des colonnes sur la feuille d’origine, tu peux les mapper ici

# -----------------------
# 7. KPIs & Log d’import
# -----------------------
nb_contacts = len(df_contacts_out)
nb_prospects = (df_contacts_out["Type"]=="Prospect").sum()
nb_membres = (df_contacts_out["Type"]=="Membre").sum()
taux_conversion = (nb_membres / max(1, (nb_membres + nb_prospects))) * 100

import json
import time
log = {
    "timestamp": time.strftime("%Y-%m-%d %H:%M:%S"),
    "source_file": INPUT_FILE,
    "output_file": OUTPUT_FILE,
    "counts": {
        "contacts": int(nb_contacts),
        "interactions": int(len(df_inter)),
        "evenements": int(len(df_events)),
        "participations": int(len(df_parts)),
        "paiements": int(len(df_pay)),
        "certifications": int(len(df_cert)),
    },
    "kpi": {
        "taux_conversion_%": round(taux_conversion, 2),
        "prospects": int(nb_prospects),
        "membres": int(nb_membres)
    },
    "notes": "Règles d’inférence : évènements/participations depuis notes ; paiements: Mobile Money réglé si montant trouvé ; impayé si 'impay' détecté."
}

# (Optionnel) tu peux aussi écrire un JSON à côté
Path("import_log.json").write_text(json.dumps(log, ensure_ascii=False, indent=2), encoding="utf-8")

# -----------------------
# 8. Export Excel multi-onglets (compatible Admin → Import Excel multi-onglets)
# -----------------------
with pd.ExcelWriter(OUTPUT_FILE, engine="openpyxl") as w:
    df_contacts_out.to_excel(w, sheet_name="contacts", index=False)
    df_inter.to_excel(w,        sheet_name="interactions", index=False)
    df_events.to_excel(w,       sheet_name="evenements", index=False)
    df_parts.to_excel(w,        sheet_name="participations", index=False)
    df_pay.to_excel(w,          sheet_name="paiements", index=False)
    df_cert.to_excel(w,         sheet_name="certifications", index=False)

print(f"✅ Fichier généré : {OUTPUT_FILE}")
print(json.dumps(log, ensure_ascii=False, indent=2))
