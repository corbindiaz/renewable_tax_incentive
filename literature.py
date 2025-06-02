from typing import Optional, Type
from langchain.tools import BaseTool
from langchain_core.vectorstores import VectorStore
from langchain_core.output_parsers import JsonOutputParser
from prompts.prompts import HyDE_post_retrieval, HyDE_pre_retrieval, HyDE_post_retrieval_xml
from langchain_core.language_models import BaseChatModel

from pydantic import BaseModel, Field
import pandas as pd
import json

from dotenv import load_dotenv
load_dotenv()

# Pre / post retrieval processing
def pre_retrieval_processing(llm_json, x, section_type):
    PreRetrievalChain = HyDE_pre_retrieval | llm_json | JsonOutputParser()
    hypo_y = PreRetrievalChain.invoke({"query": x, "section_type": section_type})
    return list(hypo_y.values())

def post_retrieval_processing(llm_json, query, section_type, y):
    PostRetrievalChain = HyDE_post_retrieval | llm_json | JsonOutputParser()
    y2 = PostRetrievalChain.invoke({"query": query, "section_type": section_type, "search_results": y})
    y3 = y2["summary"] + "\n\nReferences:\n"
    for ref in y2["references"]:
        doi = ref.get("doi", "")
        if doi:
            y3 += f"\n{ref['id']}. [{ref['ref']}]({ref['doi']})"
        else:
            y3 += f"\n{ref['id']}. {ref['ref']}"
    return y3

def post_retrieval_processing_xml(llm, query, search_results):
    """Process search results into XML format policy analysis"""
    response = llm.invoke(HyDE_post_retrieval_xml.invoke({
        "query": query,
        "search_results": search_results
    })).content

    # Extract content between XML tags
    import re
    references_match = re.search(r'<references>(.*?)</references>', response, re.DOTALL)
    summary_match = re.search(r'<summary>(.*?)</summary>', response, re.DOTALL)

    if not references_match or not summary_match:
        raise ValueError("Invalid XML format in response")

    references = references_match.group(1).strip()
    summary = summary_match.group(1).strip()

    # Format output
    formatted_refs = []
    for ref_match in re.finditer(r'<reference>(.*?)</reference>', references, re.DOTALL):
        ref = ref_match.group(1)
        id_match = re.search(r'<id>(.*?)</id>', ref)
        ref_text_match = re.search(r'<ref>(.*?)</ref>', ref)
        doi_match = re.search(r'<doi>(.*?)</doi>', ref)

        if id_match and ref_text_match:
            ref_id = id_match.group(1).strip().replace('[', '').replace(']', '')
            ref_text = ref_text_match.group(1).strip()
            doi = doi_match.group(1).strip() if doi_match else ""

            if doi:
                formatted_refs.append(f"{ref_id}. [{ref_text}]({doi})")
            else:
                formatted_refs.append(f"{ref_id}. {ref_text}")

    summary = [i.strip() for i in summary.split("\n") if i.strip()]
    response = "\n\n".join(summary) + "\n\nReferences:\n" + "\n".join(formatted_refs)
    print(response)
    return response

# Format policy results
def __format_output_df_as_prompt__(df):
    prompt = []
    for index, row in df.iterrows():
        policy_name = row.get('policy_name', 'Unknown Policy')
        state = row.get('state', 'Unknown State')
        date = row.get('date', 'Unknown Date')

        ref = f"{policy_name} ({state}, {date})"
        text = row['content']
        prompt.append({"ref": ref, "text": text})
    prompt = json.dumps(prompt, indent=4)
    return prompt

# Search + format
def __search_and_format__(PVS, search_keywords, k=3, state=None, date=None, policy_name=None):
    filter_conditions = []
    if state:
        filter_conditions.append({"state": state})
    if date:
        filter_conditions.append({"date": date})
    if policy_name:
        filter_conditions.append({"policy_name": policy_name})
    filter_query = {"$and": filter_conditions} if filter_conditions else None

    if type(search_keywords) == str:
        search_keywords = [search_keywords]

    df = []
    for search_keyword in search_keywords:
        output = PVS.similarity_search(search_keyword, filter=filter_query, k=k)
        df_temp = pd.DataFrame([i.metadata | {'content': i.page_content} for i in output])
        df.append(df_temp)
    df = pd.concat(df, ignore_index=True)

    if df.shape[0] > 0:
        output = df.sort_values(['policy_name', 'date']).drop_duplicates(['policy_name', 'date']).reset_index(drop=True)
        formatted_prompt = __format_output_df_as_prompt__(output)
    else:
        formatted_prompt = "No search results found."
    return formatted_prompt

# INPUT class
from typing import Literal
class SearchPolicyAdvancedInput(BaseModel):
    query: str = Field(..., description="The search query to find relevant energy policy documents")
    k: int = Field(10, description="A larger value provides more results", ge=1)
    state: Optional[str] = Field(None, description="Filter results to only include policies from a specific state")
    date: Optional[str] = Field(None, description="Filter results to only include policies from a specific date")
    policy_name: Optional[str] = Field(None, description="Filter results to only include a specific policy")

# ADVANCED TOOL — WITH EXAMPLES
class SearchPolicyAdvancedTool(BaseTool):
    name: str = "search_policy"
    description: str = """
    Function: Performs an advanced semantic search across US energy policy documents to find relevant information.
    Output: A comprehensive policy analysis with:
    - Relevant policy sections and quotes
    - Full citations with policy names and dates
    - URLs when available
    - Contextual summary connecting the results to the query
    Note: This tool specializes in US energy policy documents only.
    """
    args_schema: Type[BaseModel] = SearchPolicyAdvancedInput
    vectorstore: VectorStore
    llm_json: BaseChatModel
    example_vectorstore: Optional[VectorStore] = None  # NEW PARAM

    def _run(self, query: str, k: int = 10, state: str = None, date: str = None, policy_name: str = None):
        response = {}
        try:
            # STEP 1 — Retrieve POLICY documents
            hypo_ys = [query]
            y = __search_and_format__(self.vectorstore, hypo_ys, k, state, date, policy_name)

            # STEP 2 — Retrieve EXAMPLES from prompt-validation
            formatted_examples = ""
            if self.example_vectorstore:
                retrieved_examples = self.example_vectorstore.similarity_search(query, k=3)
                for i, doc in enumerate(retrieved_examples):
                    formatted_examples += f"Example {i+1}:\n"
                    formatted_examples += f"Prompt: {doc.metadata.get('prompt', '')}\n"
                    formatted_examples += f"Response: {doc.metadata.get('response', '')}\n\n"

            # STEP 3 — Build enhanced input for HyDE post-processing
            if y == "No search results found.":
                response['response'] = y
            else:
                search_results_with_examples = f"""
Examples of good responses:

{formatted_examples}

Now, here are the policy documents:

{y}
"""
                # STEP 4 — Call post_retrieval_processing_xml
                response['response'] = post_retrieval_processing_xml(
                    self.llm_json,
                    query,
                    search_results_with_examples
                )

        except Exception as e:
            response['response'] = "{}: {}".format(type(e).__name__, str(e))
        finally:
            return response

# VANILLA TOOL
class SearchPolicyVanillaInput(BaseModel):
    query: str = Field(..., description="The search term")
    k: int = Field(3, description="Number of results to return")
    state: Optional[str] = Field(None, description="State to filter by")
    date: Optional[str] = Field(None, description="Date to filter by")
    policy_name: Optional[str] = Field(None, description="Policy name to filter by")

class SearchPolicyVanillaTool(BaseTool):
    name: str = "search_policy_vanilla"
    description: str = """
    Function: Performs a search for US energy policy documents.
    Input:
    - query: The main search term (required)
    - k: Number of results to return (default: 3)
    - state: State to filter by (optional)
    - date: Date to filter by (optional)
    - policy_name: Policy name to filter by (optional)
    Output: A formatted list of search results including policy details and relevant text sections.
    """
    args_schema: Type[BaseModel] = SearchPolicyVanillaInput
    vectorstore: VectorStore

    def _run(self, query: str, k: int = 3, state: str = None, date: str = None, policy_name: str = None):
        response = {}
        try:
            response['response'] = __search_and_format__(self.vectorstore, query, k, state, date, policy_name)
        except Exception as e:
            response['response'] = "{}: {}".format(type(e).__name__, str(e))
        finally:
            return response

# INITIALIZE TOOLS
from langchain_pinecone import PineconeVectorStore
from langchain_openai import OpenAIEmbeddings
from langchain_google_vertexai.model_garden import ChatAnthropicVertex

# LLM init
from langchain_openai import ChatOpenAI
llm_json = ChatOpenAI(
    model="gpt-4o",
    temperature=0,
    max_tokens=8192,
    timeout=None,
    max_retries=2,
)

# Policy vectorstore
policy_vectorstore = PineconeVectorStore.from_existing_index(
    embedding=OpenAIEmbeddings(model="text-embedding-3-small"),
    index_name="energygpt-group4"
)

# Example vectorstore
example_vectorstore = PineconeVectorStore.from_existing_index(
    embedding=OpenAIEmbeddings(model="text-embedding-3-small"),
    index_name="prompt-validation"
)

# Instantiate tools
search_policy_advanced_tool = SearchPolicyAdvancedTool(
    vectorstore=policy_vectorstore,
    llm_json=llm_json,
    example_vectorstore=example_vectorstore
)

search_policy_vanilla_tool = SearchPolicyVanillaTool(
    vectorstore=policy_vectorstore
)