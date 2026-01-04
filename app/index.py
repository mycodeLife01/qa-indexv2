import os
import hashlib
from pathlib import Path
from llama_index.core.node_parser import SentenceSplitter
from llama_index.core.storage.docstore import SimpleDocumentStore
import requests
import tempfile
from llama_index.core import SimpleDirectoryReader
from llama_index.core import Document
from llama_index.core.ingestion import IngestionPipeline, IngestionCache

# from llama_index.core.extractors import TitleExtractor
from llama_index.embeddings.google_genai import GoogleGenAIEmbedding
from llama_index.vector_stores.chroma import ChromaVectorStore
import chromadb
from llama_index.storage.kvstore.redis import RedisKVStore as RedisCache
from llama_index.storage.docstore.redis import RedisDocumentStore
from dotenv import load_dotenv

load_dotenv()

redis_uri = os.getenv("RAG_REDIS_URL")

# assign vector store - use ChromaDB HTTP server
chroma_host = os.getenv("CHROMA_HOST", "localhost")
chroma_port = os.getenv("CHROMA_PORT", "8000")
remote_db = chromadb.HttpClient(host=chroma_host, port=int(chroma_port))
chroma_collection = remote_db.get_or_create_collection("my-qa-llama")
vector_store = ChromaVectorStore(chroma_collection=chroma_collection)

# assign cache management (使用 redis_uri 会自动创建同步和异步客户端)
cache = IngestionCache(
    cache=RedisCache(redis_uri=redis_uri),
    collection=os.getenv("LLAMA_REDIS_CACHE_NAME"),
)

# assign docstore (使用相同的 redis_uri)
docstore = RedisDocumentStore(
    redis_kvstore=RedisCache(redis_uri=redis_uri),
    namespace=os.getenv("LLAMA_DOC_STORE_NAME"),
)

# Define unstable metadata keys
UNSTABLE_METADATA_KEYS = [
    "file_path",
    "file_name",
    "file_type",
    "file_size",
    "creation_date",
    "last_modified_date",
    "last_accessed_date",
]


def handle_docs_metadata(
    docs: list[Document],
    unstable_keys: list[str],
    source_url: str,
    content_hash: str,
    file_type: str,
) -> None:
    url_hash = hashlib.sha256(source_url.encode()).hexdigest()[:16]

    # 为每个文档设置稳定的 ID 并清理不稳定的 metadata
    for doc in docs:
        doc.id_ = url_hash  # 设置稳定的文档 ID

        for key in unstable_keys:
            doc.metadata.pop(key, None)

        # 添加稳定的 metadata
        doc.metadata["source_url"] = source_url
        doc.metadata["doc_id"] = url_hash
        doc.metadata["doc_content_hash"] = content_hash
        doc.metadata["doc_type"] = file_type


def process(url: str, content_hash: str, file_type: str):
    try:
        # download user file as temp
        with tempfile.NamedTemporaryFile(
            delete=False, suffix=Path(url).suffix
        ) as tmp_file:
            print("Start processing task")
            print(f"Downloading file from {url}...")
            with requests.get(url, stream=True) as r:
                r.raise_for_status()
                for chunk in r.iter_content(chunk_size=8192):
                    tmp_file.write(chunk)
            tmp_file_path = tmp_file.name
            print(f"File downloaded to temporary path: {tmp_file_path}")

        # load data
        loader = SimpleDirectoryReader(input_files=[tmp_file_path])
        documents = loader.load_data()

        # Docs Cleaning
        handle_docs_metadata(
            documents, UNSTABLE_METADATA_KEYS, url, content_hash, file_type
        )

        # index
        pipeline = IngestionPipeline(
            transformations=[
                SentenceSplitter(chunk_size=512, chunk_overlap=128),
                # TitleExtractor(),
                GoogleGenAIEmbedding(
                    model_name="gemini-embedding-001",
                    embed_batch_size=100,
                    api_key=os.getenv("GOOGLE_API_KEY"),
                ),
            ],
            vector_store=vector_store,
            docstore=docstore,
            cache=cache,
        )

        print("Start executing ingestion pipeline")
        pipeline.run(documents=documents)
        print("task done successfully")

    except Exception as e:
        import traceback

        print(f"Task error: {e}")
        print("Full traceback:")
        traceback.print_exc()
        raise e
    finally:
        print(f"Cleaning up temporary file: {tmp_file_path}")
        os.remove(tmp_file_path)
