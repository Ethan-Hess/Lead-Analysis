import ollama
import json


def generate_column_name(sample_data):
    sample_values_str = json.dumps(sample_data)

    # Craft the prompt for the LLM
    prompt = f"""You are given 20 sample values from a data column:
{sample_values_str}

Your task is to analyze these values and determine if they consistently represent one of the following data categories:

- First_Name  
- Last_Name  
- Email  
- Phone_Number  
- Work_Occupation  
- Business_webpage  
- LinkedIn  

Instructions:
1. Choose the **single most appropriate** category name from the list above if the list clearly match it.
2. If the values do **not clearly and consistently** match any category, respond with: **UNNAMED**
3. Do not explain your reasoning. Respond with only the category name.

Output Format:  
Suggested Name:"""


    try:
        # Send the request to the local Ollama server
        response = ollama.chat(
            model='llama3', # Use the model name from Ollama
            messages=[
                {'role': 'user', 'content': prompt}
            ],
            options={
                'temperature': 0.0, # Lower temperature for more deterministic output
                'num_predict': 15 # Limit the number of tokens to generate (adjust if needed)
            }
        )

        # Extract the suggested name
        suggested_name = response['message']['content'].strip()

        return suggested_name

    except Exception as e:
        print(f"\nError interacting with Ollama: {e}")