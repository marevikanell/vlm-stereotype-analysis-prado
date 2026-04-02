# Gendered Stereotypes in Art  
### Computational Analysis of Religious and Secular Iconography

This repository contains the code, intermediate outputs, and results for a thesis project analyzing gendered stereotype content in historical artworks from the Museo del Prado.

The project applies **vision–language models (VLMs)** and the **Stereotype Content Model (SCM)** to quantify warmth–competence representations of human figures across religious and secular contexts, with additional analysis of animal iconography.

---

## 📌 Project Overview

The study investigates three main questions:

- **H1 — Gender Asymmetry**  
  Are female figures depicted as warmer and less competent than male figures?

- **H2 — Religious Context**  
  Does religious iconography amplify or reduce the gender gap?

- **H3 — Animal Iconography**  
  Do animals (e.g., lambs, lions) reinforce or modify gendered evaluations?

The analysis is conducted on **~19,000 human figures extracted from ~6,000 artworks**, using automated captioning and dictionary-based scoring.

---

## ⚙️ Methodology

The pipeline consists of:

1. **Data Construction**
   - Prado artwork metadata + iconography tags
   - Image scraping and preprocessing
   - SQLite relational database

2. **Vision–Language Processing**
   - Qwen2.5-VL used to generate figure-level descriptions
   - Structured prompts to extract posture, traits, and gender

3. **Stereotype Measurement**
   - SADCAT dictionary (AGRUPA project)
   - Warmth and Competence scoring
   - Asymmetry index: `Warmth − Competence`

4. **Statistical Analysis**
   - Welch’s t-tests and effect sizes
   - OLS regression models (H1–H3)
   - Interaction effects (Gender × Religion × Fauna)
   - Robustness checks (century fixed effects, clustering)

---
## 📁 Repository Structure

```bash
.
├── notebooks/          # Main analysis notebooks (EDA, validation, H1–H3)
├── scripts/            # Helper scripts and pipeline utilities
├── src/                # Core processing modules
├── r_files/            # R scripts for SADCAT scoring
├── caption_outputs/    # Intermediate outputs (VLM-generated descriptions)
├── figures/            # Plots used in the thesis
├── README.md
└── requirements.lock.txt
```

---

## 📊 Data Availability

The full dataset (SQLite database and images) is **not included** in this repository due to:

- Data size constraints
- Licensing restrictions (Museo del Prado collection)

### ✔️ What is included:
- Sample analysis dataset (`analysis_dataset.csv`)
- All processed outputs required to reproduce results
- Figures and model outputs used in the thesis

### ❗ Important:
This repository is **not fully reproducible end-to-end** without the original database and images.  
However, it provides full transparency of:
- Data processing steps  
- Modeling pipeline  
- Statistical analysis  

---

## 🚀 How to Use

### 1. Install dependencies
```bash
pip install -r requirements.lock.txt
```
### 2. Explore notebooks
	•	02_sadcat_eda.ipynb → exploratory analysis
	•	03_validation.ipynb → pipeline validation
	•	04_h1testing_gender.ipynb → baseline gender effects
	•	05_h2testing_religion.ipynb → religious interaction
	•	06_h3animal_clustering.ipynb → animal cluster construction
	•	07_h3_testing_animal_symbolism.ipynb → final models

### ⚠️ Reproducibility Notes
	•	The repository uses precomputed outputs (e.g., captions, scores)
	•	Some scripts assume access to the original SQLite database
	•	Paths may need to be adjusted for local execution

### 📬 Contact

For questions or collaboration:
	•	GitHub: @marevikanell￼

