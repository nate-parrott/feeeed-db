"""Helper module for making JSON-based LLM API calls with structured output support"""

import os
import json
from openai import OpenAI
import socket
from typing import Dict, Any, Optional

def query_llm_json(
    prompt: str, 
    model: str = 'openai/gpt-4o-mini',
    json_schema: Optional[Dict[str, Any]] = None
) -> dict:
    """
    Query LLM with a prompt and get JSON response
    
    Args:
        prompt: The prompt to send to the LLM
        model: The model to use for the query
        json_schema: Optional JSON schema to enforce structured output
    
    Returns:
        dict: Parsed JSON from LLM response
    
    Raises:
        ValueError: If LLM returns invalid JSON
    """
    # Make GPT API call using OpenRouter 
    client = OpenAI(
        base_url="https://openrouter.ai/api/v1",
        api_key=os.environ.get("OPENROUTER_API_KEY")
    )
    
    # Set up response format based on whether schema is provided
    if json_schema:
        response_format = {
            "type": "json_schema",
            "json_schema": json_schema
        }
    else:
        response_format = {"type": "json_object"}
    
    # response = client.chat.completions.create(
    #     model=model,
    #     response_format=response_format, 
    #     messages=[{"role": "user", "content": prompt}]
    # )

    # stream
    response = client.chat.completions.create(
        model=model,
        response_format=response_format, 
        messages=[{"role": "user", "content": prompt}],
        stream=False
    )
    # ChatCompletionChunk(id='gen-1740714132-v1NXu90HyneyoSrso4FP', choices=[Choice(delta=ChoiceDelta(content=' Development', function_call=None, refusal=None, role='assistant', tool_calls=None), finish_reason=None, index=0, logprobs=None, native_finish_reason=None)], created=1740714132, model='openai/gpt-4o', object='chat.completion.chunk', service_tier=None, system_fingerprint='fp_f9f4fb6dbf', usage=None, provider='OpenAI')
    # for response in response:
    #     import sys
    #     sys.stdout.write(response.choices[0].delta.content)
    #     sys.stdout.flush()
        # print(response.choices[0].message.content)

    # print(response)
    
    try:
        return json.loads(response.choices[0].message.content, strict=False)
    except json.JSONDecodeError as e:
        raise ValueError(f"LLM returned invalid JSON: {e}")

if __name__=='__main__':
    p = "This is a test. Respond with a JSON object containing one key, hello, with the value 'world'"
    print(query_llm_json(p))