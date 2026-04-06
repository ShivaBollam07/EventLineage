# EventLineage - How It Works

A complete walkthrough of what happens when you upload a DICOM study.

---

## The Setup

There are **6 services** running in Docker:

| Service | Port | Role |
|---------|------|------|
| **dicom-gateway** | 5001 | Python Flask app. Receives DICOM uploads from you |
| **dicom-processor** | 5002 | Python Flask app. Processes DICOM studies in the background |
| **Kafka** | 9092 | Message broker. Carries messages between services |
| **EventLineage API** | 8080 | Go backend. Reads lineage events from Kafka, stores them in Neo4j, serves the UI |
| **Neo4j** | 7474 | Graph database. Stores messages as nodes and causal links as edges |
| **React UI** | 3000 | Frontend dashboard. Visualizes chains as interactive graphs |

---

## The Flow - Step by Step

### You send a request

```bash
curl -X POST http://localhost:5001/api/dicom/upload \
  -H "Content-Type: application/json" \
  -d '{
    "patientName": "John Smith",
    "modality": "CT",
    "studyDescription": "Chest CT Scan",
    "numSeries": 4,
    "numInstances": 120
  }'
```

This hits **dicom-gateway** on port 5001.

---

### Step 1: dicom-gateway receives the study

The gateway generates two key IDs:

- **studyId** — identifies this specific study (e.g. `b2ced2b7-e487-...`)
- **correlationId** — ties the **entire chain** together across all services (e.g. `e2de6cff-057d-...`)

Then it publishes a **lineage event** to the Kafka topic `_lineage.events`:

```json
{
  "eventType": "produced",
  "messageId": "826a71f2-...",
  "parentId": null,
  "correlationId": "e2de6cff-...",
  "rootMessageId": "826a71f2-...",
  "serviceOrigin": "dicom-gateway",
  "topic": "dicom.incoming",
  "causationReason": "StudyReceived",
  "status": "ok",
  "timestamp": "2026-04-05T10:39:29Z",
  "payloadPreview": "{\"studyId\":\"b2ced2b7-...\",\"modality\":\"CT\",\"instances\":120}"
}
```

Key things to notice:
- `parentId` is **null** because this is the **root** — the first event in the chain
- `correlationId` is the thread that connects every event in this chain
- `messageId` and `rootMessageId` are the same (it's the root)

**File:** `services/dicom-gateway/app.py`, lines 101-113

---

### Step 2: dicom-gateway validates the study

The gateway checks the DICOM headers (modality, patient ID, etc.) and publishes a second lineage event:

```json
{
  "eventType": "produced",
  "messageId": "f3a8b1c2-...",
  "parentId": "826a71f2-...",
  "correlationId": "e2de6cff-...",
  "rootMessageId": "826a71f2-...",
  "serviceOrigin": "dicom-gateway",
  "topic": "dicom.validated",
  "causationReason": "StudyValidated",
  "payloadPreview": "{\"studyId\":\"b2ced2b7-...\",\"valid\":true,\"checks\":[\"header\",\"modality\",\"patient-id\"]}"
}
```

Key things to notice:
- `parentId` now points to **Step 1's messageId** — this creates the causal link
- Same `correlationId` — they belong to the same chain
- Different `messageId` — each step is a unique message

**File:** `services/dicom-gateway/app.py`, lines 118-129

---

### Step 3: dicom-gateway forwards to Kafka

The gateway publishes the actual study data to a **separate** Kafka topic called `dicom.incoming`:

```json
{
  "studyId": "b2ced2b7-...",
  "correlationId": "e2de6cff-...",
  "patientName": "John Smith",
  "modality": "CT",
  "rootMessageId": "826a71f2-...",
  "parentMessageId": "f3a8b1c2-..."
}
```

This is **not** a lineage event. This is the actual business data being passed to the next service. The `rootMessageId` and `parentMessageId` are embedded so the processor knows where it fits in the chain.

You get back a response:
```json
{
  "message": "Study uploaded and validated",
  "studyId": "b2ced2b7-...",
  "correlationId": "e2de6cff-...",
  "lineageUrl": "/chains/e2de6cff-..."
}
```

**File:** `services/dicom-gateway/app.py`, lines 132-146

---

### Step 4: dicom-processor picks up the study

The **dicom-processor** has a background thread running a Kafka consumer on the `dicom.incoming` topic. It receives the study data and publishes a lineage event:

```json
{
  "eventType": "produced",
  "messageId": "d7e9f4a1-...",
  "parentId": "f3a8b1c2-...",
  "correlationId": "e2de6cff-...",
  "rootMessageId": "826a71f2-...",
  "serviceOrigin": "dicom-processor",
  "topic": "dicom.processing",
  "causationReason": "StudyProcessing",
  "payloadPreview": "{\"studyId\":\"b2ced2b7-...\",\"stage\":\"parsing-dicom-tags\"}"
}
```

Notice: `serviceOrigin` changed from `dicom-gateway` to `dicom-processor`. The chain now spans two services.

**File:** `services/dicom-processor/app.py`, lines 67-79

---

### Step 5: dicom-processor completes analysis

After processing (simulated 0.5s delay), it publishes the final lineage event:

```json
{
  "eventType": "produced",
  "messageId": "a1b2c3d4-...",
  "parentId": "d7e9f4a1-...",
  "correlationId": "e2de6cff-...",
  "rootMessageId": "826a71f2-...",
  "serviceOrigin": "dicom-processor",
  "topic": "dicom.results",
  "causationReason": "AnalysisComplete",
  "payloadPreview": "{\"studyId\":\"b2ced2b7-...\",\"findings\":\"Normal\",\"confidence\":0.95}"
}
```

**File:** `services/dicom-processor/app.py`, lines 83-95

---

## Where It Gets Stored

### Kafka topics involved

There are **two** Kafka topics at play:

| Topic | Purpose | Who writes | Who reads |
|-------|---------|-----------|----------|
| `_lineage.events` | Lineage tracking events | dicom-gateway, dicom-processor | EventLineage API (Go backend) |
| `dicom.incoming` | Actual business data | dicom-gateway | dicom-processor |

The `_lineage.events` topic is the **observability layer**. The `dicom.incoming` topic is the **business layer**. They are completely separate.

---

### EventLineage API consumes lineage events

The Go backend (`backend/internal/kafka/consumer.go`) runs a Kafka consumer on `_lineage.events`.

For each event it receives, it:
1. Deserializes the JSON into a `LineageEvent` struct
2. Validates that `messageId` and `correlationId` are present
3. Defaults `status` to `"ok"` and `rootMessageId` to `messageId` if missing
4. Calls `UpsertMessage()` to write it to Neo4j

---

### Neo4j stores it as a graph

Each lineage event becomes a **node** with the label `:Message`:

```
(:Message {
  messageId: "826a71f2-...",
  correlationId: "e2de6cff-...",
  rootMessageId: "826a71f2-...",
  parentId: null,
  serviceOrigin: "dicom-gateway",
  topic: "dicom.incoming",
  causationReason: "StudyReceived",
  status: "ok",
  retryCount: 0,
  timestamp: 2026-04-05T10:39:29Z,
  payloadPreview: "{...}"
})
```

When a message has a `parentId`, a **CAUSED** edge is created:

```
(parent:Message)-[:CAUSED {reason: "StudyValidated"}]->(child:Message)
```

The write uses `MERGE` (not `CREATE`), so it's **idempotent** — if the same event arrives twice, it won't create duplicates. If a child arrives before its parent, a stub parent node is created and filled in later.

**File:** `backend/internal/store/message_repo.go`, lines 25-99

---

### The final graph for one chain

```
[StudyReceived]  ──CAUSED──>  [StudyValidated]  ──CAUSED──>  [StudyProcessing]  ──CAUSED──>  [AnalysisComplete]
 dicom-gateway                 dicom-gateway                  dicom-processor                  dicom-processor
 depth: 0                      depth: 1                       depth: 2                         depth: 3
```

All four nodes share the same `correlationId`. This groups them into one **chain**.

---

## How the UI Displays It

### React UI fetches from the Go API

| API Endpoint | What it returns | UI Component |
|-------------|----------------|-------------|
| `GET /api/v1/services` | List of services with message counts, chain counts, failure rates | ServiceList (dashboard) |
| `GET /api/v1/chains` | List of chains with summary info (message count, status, root service) | ChainExplorer |
| `GET /api/v1/chains/:correlationId` | Full chain with all nodes and edges | ChainGraph (ReactFlow DAG) |
| `GET /api/v1/messages/:messageId` | Single message with parent and children | MessagePanel (side panel) |

### The chain detail response looks like this:

```json
{
  "correlationId": "e2de6cff-...",
  "rootMessageId": "826a71f2-...",
  "status": "completed",
  "messageCount": 4,
  "depthMax": 3,
  "retryCount": 0,
  "failureCount": 0,
  "nodes": [
    {
      "messageId": "826a71f2-...",
      "service": "dicom-gateway",
      "topic": "dicom.incoming",
      "causationReason": "StudyReceived",
      "status": "ok",
      "depth": 0
    },
    {
      "messageId": "f3a8b1c2-...",
      "service": "dicom-gateway",
      "topic": "dicom.validated",
      "causationReason": "StudyValidated",
      "status": "ok",
      "depth": 1
    },
    {
      "messageId": "d7e9f4a1-...",
      "service": "dicom-processor",
      "topic": "dicom.processing",
      "causationReason": "StudyProcessing",
      "status": "ok",
      "depth": 2
    },
    {
      "messageId": "a1b2c3d4-...",
      "service": "dicom-processor",
      "topic": "dicom.results",
      "causationReason": "AnalysisComplete",
      "status": "ok",
      "depth": 3
    }
  ],
  "edges": [
    {"from": "826a71f2-...", "to": "f3a8b1c2-...", "reason": "StudyValidated"},
    {"from": "f3a8b1c2-...", "to": "d7e9f4a1-...", "reason": "StudyProcessing"},
    {"from": "d7e9f4a1-...", "to": "a1b2c3d4-...", "reason": "AnalysisComplete"}
  ]
}
```

The React UI takes the `nodes` and `edges` arrays and renders them as an interactive directed acyclic graph (DAG) using ReactFlow. Each node is positioned based on its `depth` value. Clicking a node opens the MessagePanel sidebar showing full details.

---

## Summary Diagram

```
You (curl POST)
 │
 ▼
┌─────────────────────┐
│   dicom-gateway     │  Port 5001
│   (Python/Flask)    │
│                     │
│  1. StudyReceived   │──── lineage event ────► Kafka: _lineage.events
│  2. StudyValidated  │──── lineage event ────► Kafka: _lineage.events
│  3. Forward study   │──── business data ────► Kafka: dicom.incoming
└─────────────────────┘
                                                       │
                                                       ▼
                                              ┌─────────────────────┐
                                              │  dicom-processor    │  Port 5002
                                              │  (Python/Flask)     │
                                              │                     │
                                              │  4. StudyProcessing │── lineage ──► Kafka: _lineage.events
                                              │  5. AnalysisComplete│── lineage ──► Kafka: _lineage.events
                                              └─────────────────────┘

                    Kafka: _lineage.events
                            │
                            ▼
                  ┌─────────────────────┐
                  │  EventLineage API   │  Port 8080
                  │  (Go/Gin)           │
                  │                     │
                  │  Kafka consumer     │
                  │  reads events       │
                  │  writes to Neo4j    │
                  └────────┬────────────┘
                           │
                           ▼
                  ┌─────────────────────┐
                  │  Neo4j              │  Port 7474
                  │  (Graph Database)   │
                  │                     │
                  │  :Message nodes     │
                  │  :CAUSED edges      │
                  └────────┬────────────┘
                           │
                           ▼
                  ┌─────────────────────┐
                  │  React UI           │  Port 3000
                  │  (Nginx + React)    │
                  │                     │
                  │  Dashboard          │
                  │  Chain Explorer     │
                  │  Chain Graph (DAG)  │
                  └─────────────────────┘
```

---

## Available API Endpoints

### dicom-gateway (port 5001)
| Method | Endpoint | Description |
|--------|----------|-------------|
| POST | `/api/dicom/upload` | Upload a DICOM study (triggers the chain) |
| GET | `/api/dicom/studies` | List all uploaded studies |
| GET | `/health` | Health check |

### dicom-processor (port 5002)
| Method | Endpoint | Description |
|--------|----------|-------------|
| GET | `/api/processor/results` | List all processed results |
| POST | `/api/processor/reprocess/:studyId` | Re-trigger processing (creates retry chain) |
| GET | `/health` | Health check |

### EventLineage API (port 8080)
| Method | Endpoint | Description |
|--------|----------|-------------|
| GET | `/api/v1/chains` | List all chains (supports `?service=`, `?status=`, `?limit=`, `?offset=`) |
| GET | `/api/v1/chains/:correlationId` | Get full chain with nodes and edges |
| GET | `/api/v1/messages/:messageId` | Get single message with parent and children |
| GET | `/api/v1/services` | List all registered services with stats |
| GET | `/api/v1/services/:name` | Get stats for a specific service |
| GET | `/health` | Health check |

### Neo4j Browser (port 7474)
- Login: `neo4j` / `eventlineage`
- **Important:** Select the `neo4j` database (not `system`) before running queries
- Try: `MATCH (n:Message)-[r:CAUSED]->(m:Message) RETURN n, r, m`
