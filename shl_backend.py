from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
from typing import List, Optional
from shl1 import AssessmentSearchSystem
import os
from dotenv import load_dotenv
import uvicorn
load_dotenv()

app = FastAPI()

mongodb_uri = os.getenv('MONGODB_URI')
search_system = AssessmentSearchSystem(mongodb_uri)

class Query(BaseModel):
    query: str

class Assessment(BaseModel):
    url: str
    adaptive_support: str
    description: str
    duration: int
    remote_support: str
    test_type: List[str]

class RecommendationResponse(BaseModel):
    recommended_assessments: List[Assessment]

@app.get("/health")
async def health_check():
    return {"status": "healthy"}

@app.post("/recommend")
async def get_recommendations(query: Query):
    try:
        results = search_system.search(query.query, limit=10)
        
        recommended_assessments = []
        for result in results:
            test_types = result['test_type'].split(',') if isinstance(result['test_type'], str) else [result['test_type']]
            assessment = Assessment(
                url=result['url'],
                adaptive_support="Yes" if result['adaptive'] == "Yes" else "No",
                description=result['description'],
                duration=int(result['assessment_length']) if result['assessment_length'] else 0,
                remote_support="Yes" if result['remote_testing'] == "Yes" else "No",
                test_type=test_types
            )
            recommended_assessments.append(assessment)
        
        return RecommendationResponse(recommended_assessments=recommended_assessments)
    
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=8000)