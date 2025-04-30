import ollama
import json


def generate_column_name(sample_data):
    sample_values_str = json.dumps(sample_data)

    # Craft the prompt for the LLM
    prompt = f"""Given these 20 sample values from a data column:
{sample_values_str}

Analyze the sample values and determine if they consistently represent one of the following categories:

- First_Name
- Last_Name
- Email
- Phone_Number
- Work_Occupation
- Business_webpage
- LinkedIn

If the sample values clearly correspond to one of these categories, return ONLY the single, most appropriate column header name (one word).

If the sample values do not clearly and consistently represent any of the listed categories, return ONLY the word: UNNAMED

Suggested Name:""" # Added a label to guide the model

    # print("Sending prompt to Mistral via Ollama...")
    # print("-" * 20)
    # print(f"Prompt:\n{prompt}")
    # print("-" * 20)

    try:
        # Send the request to the local Ollama server
        response = ollama.chat(
            model='mistral:instruct', # Use the model name from Ollama
            messages=[
                {'role': 'user', 'content': prompt}
            ],
            options={
                'temperature': 0.2, # Lower temperature for more deterministic output
                'num_predict': 15 # Limit the number of tokens to generate (adjust if needed)
            }
        )

        # Extract the suggested name
        suggested_name = response['message']['content'].strip()

        # print(f"\nSuggested Column Name: {suggested_name}")
        return suggested_name

    except Exception as e:
        print(f"\nError interacting with Ollama: {e}")
        print("Please ensure the Ollama application or 'ollama serve' is running and the 'mistral:instruct' model is downloaded ('ollama pull mistral:instruct').")