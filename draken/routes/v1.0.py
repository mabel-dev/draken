from fastapi import FastAPI, HTTPException
from dataclasses import dataclass
from typing import List, Dict

app = FastAPI()

@dataclass
class FileList():
    files: List[str]

# Example index manager class (simplified for illustration)
class IndexManager:
    def __init__(self):
        self.indexes = {}  # Simplified in-memory storage for indexes

    def create_index(self, table: str, column: str, files: List[str], hint: str = None):
        key = (table, column)
        if key in self.indexes:
            raise ValueError("Index already exists")
        self.indexes[key] = files  # Simplified; replace with actual indexing logic

    def add_files_to_index(self, table: str, column: str, files: List[str]):
        key = (table, column)
        if key not in self.indexes:
            raise ValueError("Index not found")
        self.indexes[key].extend(files)  # Simplified; handle duplicates and actual indexing logic

    def delete_index(self, table: str, column: str):
        key = (table, column)
        if key in self.indexes:
            del self.indexes[key]
        else:
            raise ValueError("Index not found")

    def get_indexes(self, table: str):
        return {key[1]: files for key, files in self.indexes.items() if key[0] == table}

    def rebuild_index(self, table: str, column: str, files: List[str] = None):
        key = (table, column)
        if key not in self.indexes:
            raise ValueError("Index not found")
        self.indexes[key] = files if files else []  # Simplified; replace with actual rebuilding logic

index_manager = IndexManager()

@app.post("/index/{table}/{column}")
async def create_index(table: str, column: str, files: FileList, hint: str = None):
    try:
        index_manager.create_index(table, column, files.files, hint)
        return {"message": "Index created", "table": table, "column": column, "files": files.files}
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))

@app.patch("/index/{table}/{column}")
async def add_files_to_index(table: str, column: str, files: FileList):
    try:
        index_manager.add_files_to_index(table, column, files.files)
        return {"message": "Files added to index", "table": table, "column": column, "files": files.files}
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))

@app.delete("/index/{table}/{column}")
async def delete_index(table: str, column: str):
    try:
        index_manager.delete_index(table, column)
        return {"message": "Index deleted", "table": table, "column": column}
    except ValueError as e:
        raise HTTPException(status_code=404, detail=str(e))

@app.get("/index/{table}")
async def get_indexes(table: str):
    indexes = index_manager.get_indexes(table)
    return {"table": table, "indexes": indexes}

@app.post("/index/{table}/{column}/rebuild")
async def rebuild_index(table: str, column: str, files: FileList = None):
    try:
        index_manager.rebuild_index(table, column, files.files if files else None)
        return {"message": "Index rebuilt", "table": table, "column": column, "files": files.files if files else []}
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))

# Run the application
if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)
