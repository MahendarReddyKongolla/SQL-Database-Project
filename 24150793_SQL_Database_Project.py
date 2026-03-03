"""
Hospital Management Database Generator using Faker

Creates hospital_database.db with:
- Departments
- Doctors
- Patients (1000 rows, with missing + duplicate values)
- Appointments (composite/compound primary key)
- Treatments
- Prescriptions (composite/compound primary key)

All data is synthetic, realistic and designed to meet assignment criteria.

DATA TYPES INCLUDED:
Nominal data (categorical, no order):
    - Patients.gender
    - Doctors.specialization
    - Treatments.treatment_type
    - Departments.department_name

Ordinal data (categorical with order):
    - Patients.severity_level
        (Low < Medium < High < Critical)
    - Doctors.experience_level
        (Junior < Mid < Senior < Consultant)

Interval data (numeric scale with arbitrary zero):
    - Patients.date_of_birth
        (date → difference meaningful, zero meaningless)
    - Appointments.appointment_date
        (calendar date → no true zero)

Ratio data (numeric with true zero):
    - Doctors.salary
    - Treatments.cost
    - Prescriptions.dosage_mg
    - Prescriptions.duration_days
    - Appointments.room_number

KEYS:
Foreign Keys:
    - Doctors.department_id → Departments.department_id
    - Appointments.patient_id → Patients.patient_id
    - Appointments.doctor_id → Doctors.doctor_id
    - Treatments.patient_id → Patients.patient_id
    - Treatments.doctor_id → Doctors.doctor_id
    - Prescriptions.treatment_id → Treatments.treatment_id

Composite (Compound) Keys:
    - Appointments(patient_id, doctor_id, appointment_date)
    - Prescriptions(treatment_id, medicine_name)

"""

# IMPORTS

from faker import Faker
import sqlite3
import random
import numpy as np
import pandas as pd
from datetime import datetime, timedelta
from pathlib import Path


# SETUP

fake = Faker()
Faker.seed(101)
random.seed(101)
np.random.seed(101)

OUT_DIR = Path("./output")
OUT_DIR.mkdir(exist_ok=True, parents=True)

DB_PATH = OUT_DIR / "hospital_database.db"


# HELPER FUNCTIONS (INTERVAL DATA)

def random_birthdate(start_year=1945, end_year=2005):
    start = datetime(start_year,1,1)
    end = datetime(end_year,12,31)
    delta = end - start
    return (start + timedelta(days=random.randint(0,delta.days))).date().isoformat()

def random_appointment_date():
    start = datetime(2022,1,1)
    end = datetime(2025,12,31)
    delta = end - start
    return (start + timedelta(days=random.randint(0,delta.days))).date().isoformat()


# 1. DEPARTMENTS
departments_df = pd.DataFrame([
    {"department_id":1,"department_name":"Cardiology"},
    {"department_id":2,"department_name":"Neurology"},
    {"department_id":3,"department_name":"Orthopedics"},
    {"department_id":4,"department_name":"Pediatrics"},
    {"department_id":5,"department_name":"Oncology"}
])

# 2. DOCTORS
experience_levels = ["Junior","Mid","Senior","Consultant"]  # ORDINAL

doctors = []

for did in range(1,201):
    doctors.append({
        "doctor_id": did,
        "doctor_name": fake.name(),
        "department_id": random.randint(1,5),
        "specialization": random.choice(
            ["Heart Surgery","Brain Disorders","Bone Injury",
             "Child Care","Cancer Therapy"]
        ),
        "experience_level": random.choice(experience_levels),
        "salary": round(random.uniform(50000,250000),2)  # RATIO
    })

doctors_df = pd.DataFrame(doctors)


# 3. PATIENTS (1000 rows)
severity_levels = ["Low","Medium","High","Critical"]  # ORDINAL

patients = []

for pid in range(1,1001):
    patients.append({
        "patient_id": pid,
        "patient_name": fake.name(),
        "gender": random.choice(["Male","Female","Other"]),
        "date_of_birth": random_birthdate(),  # INTERVAL
        "severity_level": random.choice(severity_levels),
        "insurance_provider": random.choice(
            ["Aetna","BlueCross","UnitedHealth","None"]
        ),
        "contact_number": fake.phone_number(),
        "city": fake.city()
    })

patients_df = pd.DataFrame(patients)

# Inject 4% missing contact numbers
missing_idx = np.random.choice(
    patients_df.index,
    size=int(0.04 * len(patients_df)),
    replace=False
)

patients_df.loc[missing_idx,"contact_number"] = None

# Inject 2% duplicates
dup_idx = np.random.choice(
    patients_df.index,
    size=int(0.02 * len(patients_df)),
    replace=False
)

for idx in dup_idx:
    sample = patients_df.sample(1).iloc[0]
    patients_df.at[idx,"patient_name"] = sample["patient_name"]
    patients_df.at[idx,"city"] = sample["city"]


# 4. APPOINTMENTS (Composite PK)
appointments = []

for _ in range(2000):

    patient = patients_df.sample(1).iloc[0]
    doctor = doctors_df.sample(1).iloc[0]

    appointments.append({
        "patient_id": int(patient["patient_id"]),
        "doctor_id": int(doctor["doctor_id"]),
        "appointment_date": random_appointment_date(),
        "appointment_time": random.choice(
            ["09:00","11:00","14:00","16:00"]
        ),
        "room_number": random.randint(100,500)
    })

appointments_df = pd.DataFrame(appointments).drop_duplicates(
    subset=["patient_id","doctor_id","appointment_date"]
)


# 5. TREATMENTS
treatments = []

for tid in range(1,1501):
    treatments.append({
        "treatment_id": tid,
        "patient_id": random.randint(1,1000),
        "doctor_id": random.randint(1,200),
        "treatment_type": random.choice(
            ["Surgery","Therapy","Medication","Diagnostic"]
        ),
        "cost": round(random.uniform(100,20000),2),  # RATIO
        "treatment_date": random_appointment_date()
    })

treatments_df = pd.DataFrame(treatments)


# 6. PRESCRIPTIONS (Composite PK)
prescriptions = []

for _, row in treatments_df.sample(frac=0.8, random_state=42).iterrows():
    prescriptions.append({
        "treatment_id": row["treatment_id"],
        "medicine_name": fake.word().capitalize(),
        "dosage_mg": random.randint(50,1000),  # RATIO
        "duration_days": random.randint(3,30)
    })

prescriptions_df = pd.DataFrame(prescriptions).drop_duplicates(
    subset=["treatment_id","medicine_name"]
)


# CREATE DATABASE
if DB_PATH.exists():
    DB_PATH.unlink()

conn = sqlite3.connect(DB_PATH)
cur = conn.cursor()
cur.execute("PRAGMA foreign_keys = ON;")

schema = """
CREATE TABLE Departments(
    department_id INTEGER PRIMARY KEY,
    department_name TEXT NOT NULL
);

CREATE TABLE Doctors(
    doctor_id INTEGER PRIMARY KEY,
    doctor_name TEXT NOT NULL,
    department_id INTEGER,
    specialization TEXT,
    experience_level TEXT CHECK(experience_level IN
        ('Junior','Mid','Senior','Consultant')),
    salary REAL CHECK(salary >= 0),
    FOREIGN KEY(department_id) REFERENCES Departments(department_id)
);

CREATE TABLE Patients(
    patient_id INTEGER PRIMARY KEY,
    patient_name TEXT NOT NULL,
    gender TEXT CHECK(gender IN ('Male','Female','Other')),
    date_of_birth TEXT,
    severity_level TEXT CHECK(severity_level IN
        ('Low','Medium','High','Critical')),
    insurance_provider TEXT,
    contact_number TEXT,
    city TEXT
);

CREATE TABLE Appointments(
    patient_id INTEGER,
    doctor_id INTEGER,
    appointment_date TEXT,
    appointment_time TEXT,
    room_number INTEGER,
    PRIMARY KEY(patient_id, doctor_id, appointment_date),
    FOREIGN KEY(patient_id) REFERENCES Patients(patient_id),
    FOREIGN KEY(doctor_id) REFERENCES Doctors(doctor_id)
);

CREATE TABLE Treatments(
    treatment_id INTEGER PRIMARY KEY,
    patient_id INTEGER,
    doctor_id INTEGER,
    treatment_type TEXT,
    cost REAL CHECK(cost >= 0),
    treatment_date TEXT,
    FOREIGN KEY(patient_id) REFERENCES Patients(patient_id),
    FOREIGN KEY(doctor_id) REFERENCES Doctors(doctor_id)
);

CREATE TABLE Prescriptions(
    treatment_id INTEGER,
    medicine_name TEXT,
    dosage_mg INTEGER CHECK(dosage_mg > 0),
    duration_days INTEGER CHECK(duration_days > 0),
    PRIMARY KEY(treatment_id, medicine_name),
    FOREIGN KEY(treatment_id) REFERENCES Treatments(treatment_id)
);
"""

cur.executescript(schema)
conn.commit()

# Insert data
departments_df.to_sql("Departments", conn, if_exists="append", index=False)
doctors_df.to_sql("Doctors", conn, if_exists="append", index=False)
patients_df.to_sql("Patients", conn, if_exists="append", index=False)
appointments_df.to_sql("Appointments", conn, if_exists="append", index=False)
treatments_df.to_sql("Treatments", conn, if_exists="append", index=False)
prescriptions_df.to_sql("Prescriptions", conn, if_exists="append", index=False)

conn.commit()
conn.close()

print("Hospital database successfully created at:", DB_PATH)
