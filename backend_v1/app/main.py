from fastapi import FastAPI, UploadFile, File, HTTPException
from fastapi.middleware.cors import CORSMiddleware
import polars as pl
import os
from pathlib import Path

from app.services.analytics import AnalyticsService

app = FastAPI(title="Nile Corp Analytics API")

# Enable CORS for frontend
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

UPLOAD_DIR = Path("data/uploads")
UPLOAD_DIR.mkdir(parents=True, exist_ok=True)

# Global state for prototype (In production, this would be PG)
current_df = None

@app.get("/")
async def root():
    return {"message": "Welcome to Nile Corporation Sales Analytics API", "status": "online"}

@app.post("/upload")
async def upload_file(file: UploadFile = File(...)):
    global current_df
    if not file.filename.endswith(('.csv', '.xlsx', '.xls')):
        raise HTTPException(status_code=400, detail="Invalid file format. Please upload CSV or Excel.")
    
    file_path = UPLOAD_DIR / file.filename
    with open(file_path, "wb") as buffer:
        buffer.write(await file.read())
    
    try:
        current_df = AnalyticsService.process_sales_data(file_path)
        return {
            "filename": file.filename,
            "rows": len(current_df),
            "columns": current_df.columns,
            "status": "Processed successfully"
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error processing file: {str(e)}")

@app.get("/analytics/dashboard")
async def get_dashboard_data():
    global current_df
    if current_df is None:
        return {"error": "No data uploaded yet"}
    
    return {
        "trends": AnalyticsService.get_sales_trends(current_df),
        "products": AnalyticsService.get_product_revenue(current_df),
        "segments": AnalyticsService.get_customer_segmentation(current_df),
        "categories": AnalyticsService.get_categorical_distribution(current_df)
    }

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)
