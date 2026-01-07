from fastapi import FastAPI, HTTPException

app = FastAPI(title="Referential API", version="1.0")

# Mock data
TURBINE_METADATA = {
    "T123": {"turbine_id": "T123", "country": "France"},
    "T124": {"turbine_id": "T124", "country": "Belgique"},
    "T125": {"turbine_id": "T125", "country": "Espagne"},
}

@app.get("/turbine/{turbine_id}")
def get_turbine_metadata(turbine_id: str):
    metadata = TURBINE_METADATA.get(turbine_id)
    if not metadata:
        raise HTTPException(status_code=404, detail="Turbine not found")
    return metadata
