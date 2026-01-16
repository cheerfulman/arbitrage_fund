from langchain_community.document_loaders import DirectoryLoader
from langchain_text_splitters import RecursiveCharacterTextSplitter
from langchain_community.embeddings import HuggingFaceEmbeddings  # 暂时保留，可升级到langchain-huggingface
from langchain_community.vectorstores import Chroma
from langchain_community.llms import Ollama  # 暂时保留，可升级到langchain-ollama
from langchain_core.runnables import RunnablePassthrough
from langchain_core.output_parsers import StrOutputParser
from langchain_core.prompts import ChatPromptTemplate

# 1. 加载 + 分块
loader = DirectoryLoader("", glob="*.txt")
docs = loader.load()
splitter = RecursiveCharacterTextSplitter(chunk_size=500, chunk_overlap=50)
chunks = splitter.split_documents(docs)

# 2. 向量化 + 存储
embeddings = HuggingFaceEmbeddings(model_name="BAAI/bge-small-zh-v1.5")
vectorstore = Chroma.from_documents(chunks, embeddings)

# 3. RAG 链
retriever = vectorstore.as_retriever()
llm = Ollama(model="qwen2:7b")

def format_docs(docs):
    return "\n\n".join(doc.page_content for doc in docs)

# 定义提示模板，将上下文和问题组合成LLM可以理解的格式
template = """
请根据以下上下文回答问题：

{context}

问题：{question}
"""

prompt = ChatPromptTemplate.from_template(template)

# 构建RAG链，确保LLM收到的是格式化后的字符串
rag_chain = (
    {"context": retriever | format_docs, "question": RunnablePassthrough()}
    | prompt  # 添加提示模板来格式化输入
    | llm
    | StrOutputParser()
)

response = rag_chain.invoke("白银套利有什么风险？")
print(response)