# Food Desert Explorer

## Overview

Food Desert Explorer is an interactive tool that evaluates and visualizes the likelihood of food deserts across the United States. Inspired by my experiences in both rural and urban environments, this project goes beyond traditional definitions by considering not just distance to stores, but also factors like income, housing costs, walkability, vehicle ownership, and transit access. Users can input an address to see a composite food desert score, compare locations, and explore insights through an embedded blog post, all with the goal of raising awareness that food access is shaped by more than just proximity to a supermarket.

---

## Project Goals

- **Integrate** diverse datasets to provide a more comprehensive view of food access.
- **Analyze** the impact of walkability, transit, and vehicle ownership on food desert status.
- **Develop** a user-friendly Streamlit app for individuals and policymakers.

---

## Data Sources (all ran from AWS S3 pipeline to Streamlit app)

- **USDA Food Access Research Atlas:** Food desert status and related indicators at the census tract level.
- **American Community Survey (ACS) 5-Year Estimates:** Demographics, rent/cost-of-living, vehicle ownership, and transit usage by census tract.
- **EPA Smart Location Database:** National walkability and smart growth indicators.
- **CDC PLACES:** Local health data related to nutrition and chronic disease risk factors.
- **National Walkability Index (NWI):** Walkability scores derived from national transportation data.

---

## Repository Structure

- **data/**: Contains raw and processed data (excluded from version control for large files).
- **notebooks/**: Jupyter notebooks for exploratory analysis.
- **scripts/**: Python scripts for data wrangling, cleaning, and merging.
- **app/**: Streamlit application code.
- **docs/**: Documentation, including data dictionaries and methodology.

---

Please reach out for any questions/suggestions to make this tool better!

