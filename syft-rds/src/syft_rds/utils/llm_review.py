from bs4 import BeautifulSoup
from anthropic import Anthropic
import os

prompt_template = """You are given the text of a function that will be run on private data. 
You are tasked with evaluating whether the function is safe to run and whether the function is leaking private information. 
The private data will be passed as the arguments of the function. Your decision will be used on real data.

Here is the function:

<code>
{code}
</code>

After analyzing the code, make a decision on whether it is suitable to run it, or if it is a security risk or a privacy leak.

Provide a brief explanation for what the code is supposed to do as well as a motivation for your decision.
Also annotate the code with comments to make your explanation easier to follow. 
You only need to include the line number and a comment to be added in the code.
For that create a list of comments following this format: "Line [nubmer of line]: # [INFO/DANGER]: ..."

You do not need to create a comment for each line, just where it is important for the user to understand the decision.
If you find any problems, please comment at the end of the line of code with DANGER: <reason why>

Format your response as follows:

<evaluation>
<decision>[SUITABLE/SECURITY_RISK/PRIVACY_LEAK]</decision>
<explanation>
[Your brief explanation here]
</explanation>
<code_comments>
[Annotated code with comments]
</code_comments>
</evaluation>

DO NOT WRITE ANYTHING OUTSIDE THE FORMAT. 
"""

# TODO: abstract this, maybe with open router

def generate_llm_review(code: str):
    
    prompt = prompt_template.format(code=code)
    
    anthropic = Anthropic(api_key=os.environ["ANTHROPIC_API_KEY"])
    message = anthropic.messages.create(
        model="claude-3-5-sonnet-latest",
        max_tokens=1024,
        messages=[{
            "role": "user",
            "content": prompt
        }]
    )

    soup = BeautifulSoup(message.content[0].text, 'html.parser')

    explanation = soup.select('explanation')[0].get_text()
    decision = soup.select('decision')[0].get_text()
    code_comments = soup.select('code_comments')[0].get_text()
    return explanation, decision, code_comments
