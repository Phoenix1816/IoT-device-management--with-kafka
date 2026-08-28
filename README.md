
# IoT Telemetry Dashboard — Kafka Edition

A real-time IoT telemetry monitoring dashboard built with **ASP.NET Core .NET 8**, **React**, **Apache Kafka**, **SignalR**, and **MySQL**.

The project simulates an IoT telemetry pipeline where real-world weather data is collected from **Open-Meteo**, published to **Apache Kafka**, processed by the backend, persisted into **MySQL**, and then delivered to the React dashboard through **SignalR**.

---

## Architecture

The telemetry flow follows this architecture:


┌──────────────┐
│  Open-Meteo  │
│ Weather API  │
└──────┬───────┘
       │
       │ Temperature
       ▼
┌────────────────────────┐
│ TelemetrySimulationWorker │
│       .NET 8            │
└──────────┬─────────────┘
           │
           │ Publish
           ▼
┌────────────────────────┐
│     Apache Kafka       │
│        Topic:          │
│       telemetry        │
└──────────┬─────────────┘
           │
           │ Consume
           ▼
┌────────────────────────┐
│   KafkaConsumerService │
│        .NET 8           │
└──────────┬─────────────┘
           │
           ├──────────────► MySQL
           │                TelemetryLogs
           │
           │
           ▼
┌────────────────────────┐
│        SignalR         │
│    TelemetryHub        │
└──────────┬─────────────┘
           │
           │ ReceiveTelemetry
           ▼
┌────────────────────────┐
│      React Frontend    │
│   Real-Time Dashboard  │
└────────────────────────┘

---

## Features

* Real-time IoT telemetry monitoring
* Open-Meteo weather data integration
* Apache Kafka telemetry pipeline
* Kafka Producer / Consumer architecture
* Real-time communication with SignalR
* React-based monitoring dashboard
* MySQL telemetry persistence
* Device management
* Device activity tracking
* Last Seen tracking
* Configurable telemetry interval
* JWT-based authentication
* Role-based dashboard routing
* Swagger / OpenAPI support
* Docker-based Kafka infrastructure

---

## Tech Stack

### Backend

* .NET 8
* ASP.NET Core Web API
* Entity Framework Core
* MySQL
* SignalR
* Apache Kafka
* Confluent.Kafka
* JWT Authentication
* Swagger / OpenAPI
* BackgroundService

### Frontend

* React
* Vite
* JavaScript
* Axios
* SignalR JavaScript Client
* Chart.js / Recharts

### Infrastructure

* Docker
* Docker Compose
* Apache Kafka

### External Data Source

* Open-Meteo API

---

## Telemetry Pipeline

The application uses a background worker to periodically retrieve the current temperature from Open-Meteo.

For every active device, the worker creates a Kafka message:

{
  "deviceId": 21,
  "value": 23.5
}

The message is published to the Kafka topic:

telemetry


The `KafkaConsumerService` consumes these messages and:

1. Deserializes the Kafka message.
2. Validates the device ID.
3. Retrieves the active device from MySQL.
4. Creates a telemetry record.
5. Updates the device's `LastSeen` value.
6. Saves the telemetry record to MySQL.
7. Sends the processed telemetry to connected SignalR clients.

The React frontend listens for:

ReceiveTelemetry

and updates the dashboard in real time.

---

## Project Structure

IoTTelemetryDashboard/
│
├── Backend/
│   ├── BackgroundServices/
│   │   └── TelemetrySimulationWorker.cs
│   │
│   ├── Data/
│   │   └── AppDbContext.cs
│   │
│   ├── Hubs/
│   │   └── TelemetryHub.cs
│   │
│   ├── Models/
│   │   ├── Device.cs
│   │   ├── TelemetryLog.cs
│   │   └── KafkaTelemetryMessage.cs
│   │
│   ├── Services/
│   │   ├── KafkaConsumerService.cs
│   │   ├── KafkaProducerService.cs
│   │   ├── OpenMeteoService.cs
│   │   └── ...
│   │
│   ├── Services/
│   │   └── Interfaces/
│   │       ├── IKafkaProducerService.cs
│   │       └── IKafkaConsumerService.cs
│   │
│   ├── Controllers/
│   ├── appsettings.json
│   └── Program.cs
│
├── Frontend/
│   ├── src/
│   │   ├── pages/
│   │   │   └── PersonelDashboard.jsx
│   │   │
│   │   ├── services/
│   │   │   ├── api.js
│   │   │   ├── signalRService.js
│   │   │   └── ...
│   │   │
│   │   └── ...
│   │
│   ├── package.json
│   └── vite.config.js
│
├── docker-compose.yml
└── README.md

---

## Kafka Configuration

Kafka is configured through `appsettings.json`:

"Kafka": {
  "BootstrapServers": "localhost:9092"
}


The application uses the Kafka topic:

telemetry


The backend contains both producer and consumer services.

### Producer

`KafkaProducerService` publishes telemetry messages to Kafka.

TelemetrySimulationWorker
        │
        ▼
KafkaProducerService
        │
        ▼
Apache Kafka


### Consumer

`KafkaConsumerService` consumes telemetry messages.

Apache Kafka
     │
     ▼
KafkaConsumerService
     │
     ├──► MySQL
     │
     └──► SignalR

---

## Docker

Kafka infrastructure can be started using Docker Compose.

Start the infrastructure:

docker compose up -d

Check running containers:

docker ps


Stop the infrastructure:

docker compose down


The application itself can still be run directly from Visual Studio while Kafka runs inside Docker.

---

## Backend Setup

Navigate to the backend:

cd Backend

Restore dependencies:

dotnet restore


Run the backend:

dotnet run

Or start it directly from Visual Studio using:

F5


The backend exposes:

https://localhost:7071


Swagger:

https://localhost:7071/swagger


SignalR Hub:

https://localhost:7071/hubs/telemetry

---

## Frontend Setup

Navigate to the frontend:

cd Frontend


Install dependencies:

npm install

Start the development server:

npm run dev


The frontend will normally be available at:

http://localhost:5173


---

## SignalR

The React application establishes a SignalR connection to:

https://localhost:7071/hubs/telemetry


The frontend listens for:

connection.on(
    "ReceiveTelemetry",
    (data) => {
        // Update telemetry state
    }
);

This allows telemetry received by the backend to be reflected on the dashboard without requiring a page refresh.

---

## Database

The application uses MySQL for persistence.

Main entities include:

Devices
TelemetryLogs
Users
...

Telemetry records contain information such as:

DeviceId
Metric
Value
Unit
Timestamp

Device records also maintain:

LastSeen
Threshold
IsActive

---

## Authentication

The application uses JWT authentication.

The authentication flow is:


Login
  │
  ▼
JWT Token
  │
  ▼
Frontend Session
  │
  ▼
Authorization Header
  │
  ▼
ASP.NET Core API


Different user roles can be routed to their corresponding dashboard.

---

## Real-Time Dashboard

The personnel dashboard provides real-time monitoring of active devices.

Telemetry updates can be displayed through:

* Current temperature
* Device status
* Last Seen information
* Historical telemetry
* Real-time charts
* Device-specific telemetry

The dashboard receives updates through SignalR rather than repeatedly polling the backend.

---

## Configuration

Important configuration values are stored in:

Backend/appsettings.json

Example:

{
  "Simulation": {
    "IntervalMs": 1000
  },

  "Kafka": {
    "BootstrapServers": "localhost:9092"
  }
}


For production environments, sensitive configuration values should be provided through environment variables or a secure configuration system rather than committed directly to source control.

---

## Running the Complete System

### 1. Start infrastructure

docker compose up -d

Make sure Kafka is running.

### 2. Start the backend

Run the ASP.NET Core backend from Visual Studio or:

dotnet run


### 3. Start the frontend

cd Frontend
npm install
npm run dev


### 4. Open the dashboard

Navigate to the frontend URL and log in.

The telemetry pipeline should then operate as:

Open-Meteo
    ↓
.NET Telemetry Worker
    ↓
Kafka Producer
    ↓
Kafka / telemetry
    ↓
Kafka Consumer
    ↓
MySQL
    ↓
SignalR
    ↓
React Dashboard

---

## Purpose

This project was developed to explore the design of a real-time IoT telemetry monitoring system using modern backend, frontend, messaging, and real-time communication technologies.

The Kafka integration provides a dedicated messaging layer between telemetry production and telemetry processing, allowing the system architecture to be extended toward larger-scale IoT data ingestion scenarios.

---

## Future Improvements

Potential future improvements include:

* MQTT device integration
* Multiple telemetry metrics
* Real physical IoT devices
* Advanced threshold and alert management
* Email / notification alerts
* Kafka consumer groups for scalable processing
* Multiple Kafka partitions
* Telemetry aggregation
* Device-specific SignalR groups
* Historical analytics
* Dockerized backend and frontend
* Production deployment
* Monitoring and observability

---

## License

This project is intended for educational and development purposes.

