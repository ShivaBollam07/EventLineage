"""
DICOM Processor Service (Service B)
Consumes validated DICOM studies from Kafka, processes them, and emits results.

APIs:
  GET  /api/processor/results         - List all processed results
  POST /api/processor/reprocess/:id   - Re-trigger processing of a study

Background:
  Kafka consumer on dicom.incoming -> processes -> emits lineage events
"""

import os
import threading
import time
import uuid
from datetime import datetime, timezone
from flask import Flask, jsonify, request
from flask_cors import CORS
from eventlineage import LineageConsumer, LineageProducer

app = Flask(__name__)
CORS(app)

KAFKA_BOOTSTRAP = os.environ.get("KAFKA_BROKERS", "localhost:9092")
DICOM_INCOMING_TOPIC = "dicom.incoming"

# SDK-based producer
producer = LineageProducer(
    bootstrap_servers=KAFKA_BOOTSTRAP,
    service_name="dicom-processor"
)

results_db: list[dict] = []


def process_study(msg):
    """Simulate processing a DICOM study: parse, analyze, store result."""
    study_data = msg.value_json()
    correlation_id = msg.correlation_id
    study_id = study_data["studyId"]

    # Step 3: StudyProcessing — automagically linked to parent via SDK context
    producer.produce(
        topic="dicom.processing",
        value={"studyId": study_id, "stage": "parsing-dicom-tags"},
        causation_reason="StudyProcessing"
    )

    time.sleep(0.5)  # Simulate processing time

    # Step 4: AnalysisComplete
    producer.produce(
        topic="dicom.results",
        value={"studyId": study_id, "findings": "Normal", "confidence": 0.95},
        causation_reason="AnalysisComplete"
    )

    result = {
        "studyId": study_id,
        "correlationId": correlation_id,
        "patientId": study_data.get("patientId", ""),
        "modality": study_data.get("modality", ""),
        "findings": "Normal",
        "confidence": 0.95,
        "status": "completed",
        "processedAt": datetime.now(timezone.utc).isoformat(),
    }
    results_db.append(result)
    print(f"[dicom-processor] Processed study {study_id} (chain: {correlation_id})")


def kafka_consumer_loop():
    """Background thread: consume using LineageConsumer with retry on topic-not-found."""
    while True:
        try:
            consumer = LineageConsumer(
                bootstrap_servers=KAFKA_BOOTSTRAP,
                group_id="dicom-processor-group",
                service_name="dicom-processor",
                topics=[DICOM_INCOMING_TOPIC]
            )
            print("[dicom-processor] SDK Consumer started, waiting for studies...")

            with consumer:
                for msg in consumer:
                    try:
                        process_study(msg)
                    except Exception as e:
                        print(f"[dicom-processor] Error processing message: {e}")
        except Exception as e:
            print(f"[dicom-processor] Consumer error: {e}, retrying in 3s...")
            time.sleep(3)


@app.route("/health", methods=["GET"])
def health():
    return jsonify({"status": "ok", "service": "dicom-processor"})


@app.route("/api/processor/results", methods=["GET"])
def list_results():
    return jsonify({"results": results_db, "count": len(results_db)})


@app.route("/api/processor/reprocess/<study_id>", methods=["POST"])
def reprocess_study(study_id):
    """Re-trigger processing for a study (creates a new chain with retry)."""
    original = next((r for r in results_db if r["studyId"] == study_id), None)
    if not original:
        return jsonify({"error": "Study not found"}), 404

    # Manual context override for a "retry" or "reprocess" trigger
    correlation_id = str(uuid.uuid4())
    
    producer.produce(
        topic="dicom.reprocess",
        value={"studyId": study_id, "reason": "manual-reprocess"},
        correlation_id=correlation_id,
        causation_reason="ReprocessRequested",
        retry_count=1
    )

    time.sleep(0.3)

    producer.produce(
        topic="dicom.results",
        value={"studyId": study_id, "findings": "Normal", "confidence": 0.97},
        correlation_id=correlation_id,
        causation_reason="ReprocessComplete",
        retry_count=1
    )

    result = {
        "studyId": study_id,
        "correlationId": correlation_id,
        "patientId": original.get("patientId", ""),
        "modality": original.get("modality", ""),
        "findings": "Normal",
        "confidence": 0.97,
        "status": "reprocessed",
        "processedAt": datetime.now(timezone.utc).isoformat(),
    }
    results_db.append(result)

    return jsonify({
        "message": "Study reprocessed",
        "studyId": study_id,
        "correlationId": correlation_id,
    })


if __name__ == "__main__":
    consumer_thread = threading.Thread(target=kafka_consumer_loop, daemon=True)
    consumer_thread.start()

    port = int(os.environ.get("PORT", "5002"))
    app.run(host="0.0.0.0", port=port, debug=False)
