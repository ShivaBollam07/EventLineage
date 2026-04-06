import os
import uuid
from flask import Flask, jsonify, request
from flask_cors import CORS
from eventlineage import LineageProducer

app = Flask(__name__)
CORS(app)

KAFKA_BOOTSTRAP = os.environ.get("KAFKA_BROKERS", "localhost:9092")
DICOM_INCOMING_TOPIC = "dicom.incoming"

# SDK-based producer automatically handles lineage events and context
producer = LineageProducer(
    bootstrap_servers=KAFKA_BOOTSTRAP,
    service_name="dicom-gateway"
)

# In-memory store for demo
studies_db: list[dict] = []


@app.route("/health", methods=["GET"])
def health():
    return jsonify({"status": "ok", "service": "dicom-gateway"})


@app.route("/api/dicom/upload", methods=["POST"])
def upload_study():
    """
    Upload a DICOM study. Accepts JSON with patient/study info.
    Triggers the lineage chain via the EventLineage SDK.
    """
    data = request.get_json(silent=True) or {}

    study_id = str(uuid.uuid4())
    patient_id = data.get("patientId", f"PAT-{uuid.uuid4().hex[:8].upper()}")
    patient_name = data.get("patientName", "Anonymous")
    modality = data.get("modality", "CT")
    description = data.get("studyDescription", "Chest CT Scan")
    num_instances = data.get("numInstances", 45)

    study = {
        "studyId": study_id,
        "patientId": patient_id,
        "patientName": patient_name,
        "modality": modality,
        "studyDescription": description,
        "numInstances": num_instances,
        "status": "received",
    }

    # Use SDK to produce business message; it automatically handles Step 1/2 lineage
    # We'll set a custom causation reason for the upload event
    correlation_id = producer.produce(
        topic=DICOM_INCOMING_TOPIC,
        value=study,
        key=study_id,
        causation_reason="StudyReceived"
    )

    study["correlationId"] = correlation_id
    study["status"] = "validated"
    studies_db.append(study)

    return jsonify({
        "message": "Study uploaded and validated",
        "studyId": study_id,
        "correlationId": correlation_id,
        "lineageUrl": f"/chains/{correlation_id}",
    }), 201


@app.route("/api/dicom/studies", methods=["GET"])
def list_studies():
    """List all uploaded studies."""
    return jsonify({"studies": studies_db, "count": len(studies_db)})


if __name__ == "__main__":
    port = int(os.environ.get("PORT", "5001"))
    app.run(host="0.0.0.0", port=port, debug=False)
