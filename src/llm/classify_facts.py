## Script to add an llm summary and classification to a json of detected differences provided upstream in the pipeline.


# run script with:
# python src/llm/classify_facts.py

## Imports ------------------ Path lib for file paths, open ai to talk to the llm, datetime and json from strandard lib
from pathlib import Path
import importlib.util
from openai import OpenAI

import datetime 
import json

## Importing secret openAI API key :
BASE_DIR = Path(__file__).resolve().parents[2]
secrets_path = BASE_DIR / ".venv" / "secrets.py"
spec = importlib.util.spec_from_file_location("my_secrets", secrets_path)
secrets = importlib.util.module_from_spec(spec)
spec.loader.exec_module(secrets)

## Testing if importing secrets is working:
#print(secrets.TEST_STRING)

##===============================
# CONFIG:
##===============================


api_key = secrets.API_KEY
llm_small_model = "gpt-4o-mini"
today = datetime.date.today().isoformat()
## Open ai client: 
client = OpenAI(api_key=api_key)

# File path for testing purposes: 
#/Users/jkzmr/Developer/Market_watch/Market_Watch_Agent/data/snapshots/diff_test.json
testing_input_path = BASE_DIR / "data" / "snapshots" / "diff_test.json"
testing_output_path = BASE_DIR / "data" / "outputs" / f"{today}_classified_test.json"


# The prompt sent to the llm. In the future there will be more.
CLASSIFY_SYSTEM_PROMT  = """You are working as part of a Market Watch Analysis software package, focused on the Belgian savings account market. Your job for for this task is to provide the executive summary of the differences detected in the offerings of our competitors.  You'll receive a dictionnary containing the differences detected, analyse those changes, and return a clear, short, descriptive summary of those changes. Audience is banking professionals, lingo is allowed, but several departments will be involved, so keep it executive level. The output format MUST be a dict formated EXACTLY like this: 
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
                  
                  
Taxonomy group MUST be selected (one or more) among those four: Product & Structure, Communication & Positioning, News & External Signals, Other. When in doubt, think about which departments needs to know about the change.

Taxonomy categories are sub divisions of taxonomy groups. The value should be selected among :
- (For Product & Structure group) : Rate change, New product added, Product removed, Category change, Min/max amount, Eligibility / conditions, T&C modification.)
- (For Communication & Positioning group):  New marketing message, New promo (like time limited offers or campaigns), Tone / tagline / imagery, Above-the-line (press, display ads, banners), Below-the-line (direct messages, product page promotions))
- (For News & External Signals group): News articles or press release on rate changes, Regulatory update (FSMA, NBB or EU-level policy signal), Competitor statement (Strategic move or public positioning), Macro context signal(ECB decisions, inflation data, savings trends)
- Other (everything else. use only if none of the others fit.)

For taxonomy category, the requirements are looser: feel free to provide a new term if none of the ones provided fit.
impact : low, Medium, HIGH. 

The description summary for each entry MUST NOT BE longer than 25 words, preferably under 15. It should contain : name of the bank, the change observed (include the old and new value : like increased xyz from a to b) and the name of the product (omit the name of the bank if it's present in the product name)"""



## Options/ideas for later : 
# - include an extra summary of all changes at the top
# - (for later?) keep the number in your answer, so we can keep track of things 




##======================
## Functions: 
##======================

## Json helper functions: (load and write)
def open_and_load_json(input_path):
  with open(input_path, encoding="utf-8") as f:
    data = json.load(f)
    #print(data)
    return data

def dump_and_save_json(data, output_path):
  with open (output_path, "w", encoding="utf-8") as f:
    output_file = json.dump(data, f, ensure_ascii=False, indent=2)
    


## LLM functions: 

def talk_to_llm(data, prompt) :
  """Function to summarise in plain language changes detected. 
  Takes a dict, return a dict of key + short summary of changes"""
  client = OpenAI(api_key=api_key)
  response = client.chat.completions.create(
      model=llm_small_model,
      messages=[
          {"role": "system", "content": prompt},
          {"role": "user", "content": str(data)}
      ]
  )
  print("Prompting llm...")
  return json.loads(response.choices[0].message.content)
  


def get_llm_summary_fact_and_write_to_json(input_path=testing_input_path, output_path=testing_output_path, prompt=CLASSIFY_SYSTEM_PROMT):
  """Function using talks_to_llm and json helper to load a diff json, classify changes and dump it as a new json. 
  Args: input path, output path, prompt.    
  Current version uses enumarate to ensure classification and summary are attached to the proper source information. Needs improvement.
  """
  data = open_and_load_json(input_path)
  numbered_changes = {i: change for i, change in enumerate(data["changes"])}
  classifications = talk_to_llm(numbered_changes, prompt)
  result = numbered_changes
  for i in numbered_changes:
    result[i] = {**numbered_changes[i], **classifications[str(i)]}  
  dump_and_save_json(result, output_path)
  print(f"differences classified. Ouput:{output_path}")



## test run:

#get_llm_summary_fact_and_write_to_json(testing_input_path, testing_output_path, CLASSIFY_SYSTEM_PROMT)

# run script with:
# python src/llm/classify_facts.py

def main():
  get_llm_summary_fact_and_write_to_json(testing_input_path, testing_output_path, CLASSIFY_SYSTEM_PROMT)
  

if __name__ == "__main__":
    main()
