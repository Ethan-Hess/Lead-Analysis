import ollama
import json

def generate_column_name_fewshot(sample_data):
    if not isinstance(sample_data, list):
        sample_data = [sample_data]

    sample_values_str = json.dumps(sample_data, indent=2)

    # The core prompt remains similar, but less need for OVERLY strict phrasing
    # because examples do the heavy lifting for format/list adherence.
    user_prompt = f"""Analyze the following sample data from a column:
{sample_values_str}

Determine if this data consistently represents one of the following exact data category names:

- First_Name
- Last_Name
- Email
- Phone_Number
- Work_Occupation
- Business_webpage
- Company_Name
- LinkedIn
- Social_Media
- Date
- Address

Choose the single most appropriate name from the list, or respond with "UNNAMED" if none clearly fit. Respond ONLY in the 'Output Format' shown.

Output Format:
Suggested Name:"""

    try:
        response = ollama.chat(
            model='llama3',
            messages=[
                # --- FEW-SHOT EXAMPLES START ---
                {'role': 'user', 'content': """Analyze the following sample data from a column:
[
  "John",
  "Mary",
  "Robert",
  "Lisa",
  "William"
]

Determine if this data consistently represents one of the following exact data category names:

- First_Name
- Last_Name
- Email
- Phone_Number
- Work_Occupation
- Business_webpage
- Company_Name
- LinkedIn
- Social_Media
- Date
- Address

Choose the single most appropriate name from the list, or respond with "UNNAMED" if none clearly fit. Respond ONLY in the 'Output Format' shown.

Output Format:
Suggested Name:"""},
                {'role': 'assistant', 'content': 'Suggested Name: First_Name'}, # Desired output for example 1

                {'role': 'user', 'content': """Analyze the following sample data from a column:
[
  "Doe",
  "Smith",
  "Johnson",
  "Williams",
  "Brown"
]

Determine if this data consistently represents one of the following exact data category names:

- First_Name
- Last_Name
- Email
- Phone_Number
- Work_Occupation
- Business_webpage
- Company_Name
- LinkedIn
- Social_Media
- Date
- Address

Choose the single most appropriate name from the list, or respond with "UNNAMED" if none clearly fit. Respond ONLY in the 'Output Format' shown.

Output Format:
Suggested Name:"""},
                {'role': 'assistant', 'content': 'Suggested Name: Last_Name'}, # Desired output for example 2

                 {'role': 'user', 'content': """Analyze the following sample data from a column:
[
  "analyst",
  "engineer",
  "manager",
  "developer",
  "consultant"
]

Determine if this data consistently represents one of the following exact data category names:

- First_Name
- Last_Name
- Email
- Phone_Number
- Work_Occupation
- Business_webpage
- Company_Name
- LinkedIn
- Social_Media
- Date
- Address

Choose the single most appropriate name from the list, or respond with "UNNAMED" if none clearly fit. Respond ONLY in the 'Output Format' shown.

Output Format:
Suggested Name:"""},
                {'role': 'assistant', 'content': 'Suggested Name: Work_Occupation'}, # Desired output for example 3

                {'role': 'user', 'content': """Analyze the following sample data from a column:
[
  "apple",
  "banana",
  "cherry",
  "date",
  "elderberry"
]

Determine if this data consistently represents one of the following exact data category names:

- First_Name
- Last_Name
- Email
- Phone_Number
- Work_Occupation
- Business_webpage
- Company_Name
- LinkedIn
- Social_Media
- Date
- Address

Choose the single most appropriate name from the list, or respond with "UNNAMED" if none clearly fit. Respond ONLY in the 'Output Format' shown.

Output Format:
Suggested Name:"""},
                {'role': 'assistant', 'content': 'Suggested Name: UNNAMED'}, # Desired output for UNNAMED case

                # --- FEW-SHOT EXAMPLES END ---

                # --- YOUR ACTUAL PROMPT GOES LAST ---
                {'role': 'user', 'content': user_prompt}
            ],
            options={
                'temperature': 0.1, # Can slightly increase temp if 0.0 feels too rigid, but keep it low
                'num_predict': 50
            }
        )

        suggested_name = response['message']['content'].strip()

        if suggested_name.startswith("Suggested Name:"):
            suggested_name = suggested_name[len("Suggested Name:"):].strip()

        # Optional: Keep the post-processing check even with few-shot for maximum safety
        allowed_names = ["First_Name", "Last_Name", "Email", "Phone_Number",
                         "Work_Occupation", "Business_webpage", "LinkedIn", "UNNAMED"
                         , "Company_Name", "Socal_Media", "Date", "Address"]

        if suggested_name not in allowed_names:
             print(f"Warning: Model generated unexpected name '{suggested_name}'. Returning UNNAMED.")
             return "UNNAMED"

        return suggested_name

    except Exception as e:
        print(f"\nError interacting with Ollama: {e}")
        return "ERROR_CLASSIFYING"