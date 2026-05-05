## Function to classify the dict by llm

# run with:
# python src/llm/classify_facts.py


## Imports ------------------ 
from pathlib import Path
import importlib.util
import datetime

from openai import OpenAI

import json

## Importing secrets:
BASE_DIR = Path(__file__).resolve().parents[2]
secrets_path = BASE_DIR / ".venv" / "secrets.py"
spec = importlib.util.spec_from_file_location("my_secrets", secrets_path)
secrets = importlib.util.module_from_spec(spec)
spec.loader.exec_module(secrets)

## Testing secrets imports, importing api_key as a var
#print(secrets.TEST_STRING)

api_key = secrets.API_KEY
llm_small_model = "gpt-4o-mini"


today = datetime.date.today().isoformat()
print(today)


## Test variables --------------

test_dict = {
  "source": "bankshopper.be",
  "url": "https://www.bankshopper.be/vergelijk-spaarrekeningen/",
  "timestamp": "2026-05-04T18:51:02.082543",
  "date": "2026-05-04",
  "num_products": 50,
  "checksum": "24bb23d1dadd7a0d610b1acabdde10affacabcc4b1ef2cf47517aaa2c2532538",
  "products": [
    {
      "bank": "Beobank",
      "product_name": "Beobank Klassieke Spaarrekening",
      "base_rate": 0.45,
      "fidelity_premium": 0.55,
      "total_rate": 1.0,
      "account_type": "Gereglementeerde spaarrekening",
      "group": "Credit Mutuel Nord Europ",
      "country_of_group": "Frankrijk",
      "guarantee_fund": "België",
      "deposit_guarantee": "€ 100.000,00",
      "min_deposit": None,
      "max_deposit": None,
      "open_online": "Neen",
      "management_costs": "Fiscaliteit",
      "moody_rating": "Aa3",
      "sp_rating": "A",
      "fitch_rating": "A+",
      "product_sheet_url": "https://www.beobank.be/nl/particulier/media/13120/download?inline"
    }
  ]
}


test_dict_2 = {
  "source": "bankshopper.be",
  "url": "https://www.bankshopper.be/vergelijk-spaarrekeningen/",
  "timestamp": "2026-05-04T18:51:02.082543",
  "date": "2026-05-04",
  "num_products": 50,
  "checksum": "24bb23d1dadd7a0d610b1acabdde10affacabcc4b1ef2cf47517aaa2c2532538",
  "products": [
    {
      "bank": "Beobank",
      "product_name": "Beobank Klassieke Spaarrekening",
      "base_rate": 0.45,
      "fidelity_premium": 0.65,
      "total_rate": 1.0,
      "account_type": "Gereglementeerde spaarrekening",
      "group": "Credit Mutuel Nord Europ",
      "country_of_group": "Frankrijk",
      "guarantee_fund": "België",
      "deposit_guarantee": "€ 100.000,00",
      "min_deposit": None,
      "max_deposit": None,
      "open_online": "Neen",
      "management_costs": "Fiscaliteit",
      "moody_rating": "Aa3",
      "sp_rating": "A",
      "fitch_rating": "A+",
      "product_sheet_url": "https://www.beobank.be/nl/particulier/media/13120/download?inline"
    }
  ]
}


diff_dict = { 1 : 
  {
  "timestamp_old": "2026-05-04T18:51:02.082543",
  "source": "bankshopper.be",
  "product_name": "Beobank Klassieke Spaarrekening",
  "fidelity_premium_old": 0.55,
  "timestamp_new": "2026-05-04T18:51:02.082543",
  "fidelity_premium_new": 0.65,  
  }
}

#print(diff_dict)


# OPEN_AI base variables ------------- 


## Let's implement the api stuff

SYSTEM_PROMT  = """You are working as part of a Market Watch Analysis software package, focused on the Belgian savings account market. Your job for for this task is to provide the executive summary of the differences detected in the offerings of our competitors.  You'll receive a dictionnary containing the differences detected, analyse those changes, and return a clear, short, descriptive summary of those changes. Audience is banking professionals, lingo is allowed, but several departments will be involved, so keep it executive level. The output format MUST be a dict formated EXACTLY like this: 
{"0": {classification : {"taxonomy_category": "category",
                  "taxonomy_group": "group",
                  "impact": "value",
                  "description": "summary.",
                  }}}

{"1": {classification : {"taxonomy_category": "category",
                  "taxonomy_group": "group",
                  "impact": "value",
                  "description": "summary.",
                  }}} 
                  
                  
Taxonomy group can be selected (one or more) among those four (when in doubt, think about which departments needs to know about the change) : 
- Product & Structure (for things like : Rate change, New product added, Product removed, Category change, Min/max amount changed, Eligibility / conditions, T&C modification.)
- Communication & Positioning (for things like: New marketing message, New promo campaign like time limited offers or campains, Tone / tagline / imagery changes, Above-the-line (press, display ads, banners), Below-the-line(Direct messages, product page promotions)) 
- News & External Signals (news articles, press release on rate changes, Regulatory update (FSMA, NBB or EU-level policy signal), Competitor statement (Strategic move or public positioning), Macro context signal(ECB decisions, inflation data, savings trends))
- Other (everything else. use only if none of the others fit.)
Taxonomy group is what parts of the taxonomy group seems to best fit among those mentionned, or another one if it seems to fit better. Feel free to merge two categories into one if applicable.
impact : low, Medium, HIGH. 
The description summary for each entry MUST NOT BE longer than 15 words. It should contain : name of the bank, the change observed (include the old and new value : like increased xyz from a to b) and the name of the product (omit the name of the bank if it's present in the product name)"""



## Options : 
# - include an extra summary of all changes at the top
# - (for later?) keep the number in your answer, so we can keep track of things 

client = OpenAI(api_key=api_key)

response = client.chat.completions.create(
    model=llm_small_model,
    messages=[
        {"role": "system", "content": SYSTEM_PROMT},
        {"role": "user", "content": str(diff_dict)}
    ]
)


testing_input_path = BASE_DIR / "data" / "snapshots" / "diff_all_2026-05-05.json"
testing_output_path = BASE_DIR / "data" / "outputs" / f"{today}_classified.json"


def open_and_load_json(input_path):
  with open(input_path) as f:
    data = json.load(f)
    #print(data)
    return data

# input path = snapshots


def dump_and_save_json(data, output_path):
  with open (output_path, "w") as f:
    output_file = json.dump(data, f)
    
# read and save as latin 
# indent=2, ensure_ascii=False),
  #      encoding="utf-8"


def llm_fact_summary(data) :
  "Function to summarise in plain language changes detected. Takes a dict of old values and new values, return a dict of key + short summary of changes" 
  client = OpenAI(api_key=api_key)
  response = client.chat.completions.create(
      model=llm_small_model,
      messages=[
          {"role": "system", "content": SYSTEM_PROMT},
          {"role": "user", "content": str(data)}
      ]
  )
  print("Prompting llm...")
  print(response.choices[0].message.content)
  return json.loads(response.choices[0].message.content)
  


def get_llm_summary_fact(input_path):
  data = open_and_load_json(input_path)
  numbered_changes = {i: change for i, change in enumerate(data["changes"])}
  classifications = llm_fact_summary(numbered_changes)
  result = numbered_changes
  for i in numbered_changes:
    result[i] = {**numbered_changes[i], **classifications[str(i)]}
  
  
  output_path = testing_output_path
  dump_and_save_json(result, output_path)



get_llm_summary_fact(testing_input_path)





# add : impact + summary description

target : {"classification": {
        "taxonomy_category": "Rate change",
        "taxonomy_group": "Product & Structure",
        "impact": "HIGH",
        "description": "Belfius increased the base rate on its Fidelity savings account from 0.15% to 0.25%, a 10 basis point rise.",
        "impact_justification": "Direct rate increase on a top-4 competitor's savings product. Narrows the gap with ING."
      }
}



test_kbc  = {
  "source": "kbc.be",
  "url": "https://www.kbc.be",
  "timestamp": "2026-05-05T12:02:41.353581",
  "date": "2026-05-05",
  "num_products": 5,
  "checksum": "002c78dc38b05b7c090cf2055d56afdd28abbb152a54fdd3711ec1dcde39b239",
  "products": [
    {
      "bank": "KBC",
      "product_name": "1. KBC-Start2Save",
      "base_rate": 0.75,
      "fidelity_premium": 1.5,
      "total_rate": 2.25,
      "account_type": "Compte d'épargne réglementé",
      "group": "KBC Group",
      "country_of_group": "Belgique",
      "guarantee_fund": "Belgique",
      "deposit_guarantee": "€ 100.000,00",
      "min_deposit": None,
      "max_deposit": None,
      "open_online": "Oui",
      "management_costs": "Gratuit",
      "moody_rating": "Aa3",
      "sp_rating": "A+",
      "fitch_rating": None,
      "product_sheet_url": "https://multimediafiles.kbcgroup.eu/ng/published/KBC/PDF/info_spr_3590_F.pdf",
      "category": "B",
      "conditions": "Vous pouvez économiser jusqu'à 500 euros par mois maximum via un ordre d'épargne automatique mensuel."
    },
    {
      "bank": "KBC",
      "product_name": "2. Compte d'épargne KBC",
      "base_rate": 0.4,
      "fidelity_premium": 0.2,
      "total_rate": 0.6,
      "account_type": "Compte d'épargne réglementé",
      "group": "KBC Group",
      "country_of_group": "Belgique",
      "guarantee_fund": "Belgique",
      "deposit_guarantee": "€ 100.000,00",
      "min_deposit": None,
      "max_deposit": None,
      "open_online": "Oui",
      "management_costs": "Gratuit",
      "moody_rating": "Aa3",
      "sp_rating": "A+",
      "fitch_rating": None,
      "product_sheet_url": "https://multimediafiles.kbcgroup.eu/ng/published/KBC/PDF/info_spr_3591_F.pdf",
      "category": "A",
      "conditions": "Pas de conditions."
    },
    {
      "bank": "KBC",
      "product_name": "A savings account for a child (Cat. A*)",
      "base_rate": 0.4,
      "fidelity_premium": 0.2,
      "total_rate": 0.6,
      "account_type": "Savings Account for Third Parties",
      "group": "KBC Group",
      "country_of_group": "Belgique",
      "guarantee_fund": "Belgique",
      "deposit_guarantee": "€ 100.000,00",
      "min_deposit": None,
      "max_deposit": None,
      "open_online": "Oui",
      "management_costs": "Gratuit",
      "moody_rating": "Aa3",
      "sp_rating": "A+",
      "fitch_rating": None,
      "product_sheet_url": "https://multimediafiles.kbcgroup.eu/ng/published/KBC/PDF/info_spr_3770_E.pdf"
    },
    {
      "bank": "KBC",
      "product_name": "KBC Savings Account (Cat. A*)",
      "base_rate": 0.4,
      "fidelity_premium": 0.2,
      "total_rate": 0.6,
      "account_type": "Savings Account",
      "group": "KBC Group",
      "country_of_group": "Belgique",
      "guarantee_fund": "Belgique",
      "deposit_guarantee": "€ 100.000,00",
      "min_deposit": None,
      "max_deposit": None,
      "open_online": "Oui",
      "management_costs": "Gratuit",
      "moody_rating": "Aa3",
      "sp_rating": "A+",
      "fitch_rating": None,
      "product_sheet_url": "https://multimediafiles.kbcgroup.eu/ng/published/KBC/PDF/info_spr_3591_E.pdf"
    },
    {
      "bank": "KBC",
      "product_name": "Put your security deposit on a blocked account (Cat. A*)",
      "base_rate": 0.4,
      "fidelity_premium": 0.2,
      "total_rate": 0.6,
      "account_type": "Security Deposit Account",
      "group": "KBC Group",
      "country_of_group": "Belgique",
      "guarantee_fund": "Belgique",
      "deposit_guarantee": "€ 100.000,00",
      "min_deposit": None,
      "max_deposit": None,
      "open_online": "Oui",
      "management_costs": "Gratuit",
      "moody_rating": "Aa3",
      "sp_rating": "A+",
      "fitch_rating": None,
      "product_sheet_url": "https://multimediafiles.kbcgroup.eu/ng/published/KBC/PDF/info_spr_3594_E.pdf"
    }
  ]
}


kbc_diff = {
  "timestamp_old": "2026-05-05T12:02:41.353581",
  
}
# %%
