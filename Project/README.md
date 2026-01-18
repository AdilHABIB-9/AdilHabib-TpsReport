# StreamVision – Big Data Streaming Analytics Platform

## 📌 Project Overview
StreamVision is a Big Data project that simulates and analyzes user activity on a video streaming platform.  
The objective of this project is to generate **realistic large-scale data**, store it in a **PostgreSQL database**, and make it ready for **analytics, reporting, and Big Data processing**.

The project focuses on data engineering concepts such as data modeling, data generation, batch insertion, and realistic simulation of user behavior.

---

## 🎯 Objectives
- Simulate a real-world streaming platform (users, content, views, subscriptions).
- Generate large volumes of realistic data automatically.
- Store and manage structured data using PostgreSQL.
- Prepare datasets suitable for Big Data analysis and visualization.
- Apply best practices in data generation and database interaction.

---

## 🏗️ System Architecture
- **Python** for data generation
- **PostgreSQL** as the relational database
- **Faker & NumPy** for realistic data simulation
- **Batch insertions** for performance optimization

---

## 🧩 Database Schema
The database contains the following main tables:

- `users` – user profiles and subscriptions
- `content` – movies, TV shows, documentaries
- `episodes` – episodes for TV shows
- `viewing_sessions` – user viewing sessions
- `episode_viewing` – detailed episode viewing
- `ratings` – user ratings and reviews
- `watchlist` – user watchlists
- `subscription_events` – subscription lifecycle events
- `search_queries` – user search behavior

All tables are connected using **primary and foreign keys** to ensure data integrity.

---

## ⚙️ Data Generation Features
The project includes a complete data generation pipeline:

- 👤 **Users**: profiles, age groups, countries, subscriptions
- 🎬 **Content**: movies, TV shows, genres, ratings, actors
- 📺 **Viewing Sessions**: platforms, devices, quality, duration
- ⭐ **Ratings & Reviews**: realistic rating distributions
- 🔍 **Search Queries**: keywords, filters, clicked content
- 💳 **Subscription Events**: upgrades, downgrades, cancellations

The data is generated using realistic probability distributions to mimic real user behavior.

---

## 🚀 How to Run the Project

### 1️⃣ Requirements
- Python 3.9+
- PostgreSQL
- Required Python libraries:
```bash
pip install psycopg2 pandas numpy faker tqdm


2️⃣ Database Configuration

Edit the PostgreSQL configuration inside the script:

DB_CONFIG = {
    'host': 'localhost',
    'port': 5432,
    'database': 'streamvision',
    'user': 'postgres',
    'password': 'YOUR_PASSWORD'
}

3️⃣ Run Data Generation

Execute the main Python script:

python generate_streaming_data.py


⚠️ Warning: The script contains a database flush option that deletes all existing data.

📊 Expected Data Volume 

10,000 users

5,000 contents

100,000 viewing sessions

30,000 ratings

25,000 search queries

15,000 subscription events

These values can be adjusted in the script.

🧠 Skills & Concepts Applied

Big Data simulation

Data modeling & normalization

Batch database operations

Realistic data distributions

Python–PostgreSQL integration

Data engineering best practices

📚 Academic Context

This project was developed as part of a Big Data / Data Engineering academic module.
It demonstrates the ability to design, implement, and populate a complete data system suitable for analytical processing.

👨‍🎓 Author

Student Name: Adil
Field: Big Data Engineering
Academic Year: 2024–2025

✅ Conclusion

StreamVision provides a solid foundation for Big Data analytics projects by offering realistic, large-scale datasets and a well-structured database design. It can be extended with dashboards, machine learning models, or real-time analytics tools.