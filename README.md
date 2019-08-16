# HCUP National Readmission Database Project
A datascience project using the HCUP National Readmission Database to predict hospital readmission

### Requirements:
1. The [conda](https://www.anaconda.com/distribution/) package manager.
2. PostgreSQL 10.6 on x86_64-pc-linux-gnu, compiled by gcc (GCC) 4.8.3 20140911 (Red Hat 4.8.3-9), 64-bit

   * Other versions of PostgreSQL may work but none have been tested.

3. User with `CREATE`, `INSERT`, `SELECT`, `UPDATE`, and `DELETE` privileges.
4. An existing database with `public` schema.
5. The 2016 NRD data available for purchase from [HCUP](https://www.distributor.hcup-us.ahrq.gov/Databases.aspx).


### Instructions
1. Clone this repository:
   ```
   git clone https://github.com/tadtenacious/nrd_db_proj.git
   ```
2. Create a virtual environment with the required libraries:
```
    conda env create --name nrd -f=env.yml
```
3. Store the 3 csv files from HCUP in the data directory.
4. Activate the virtual environment:
   ```
   conda activate nrd
   ```
5. Create the configuration file for connecting to the database.
   ```
   python setup_config.py
   ```
6. Run the tests. This is done from the parent directory, so no need to cd into `tests`.
   ```
   python -m pytest
   ```
7. Load the data to the database and perform most of the feature engineering.
   ```
   python etl.py
   ```
8. Extract the data.
   `python export.py --sample` exports a 1% sample. `python export.py` exports the full 13,961,484 row data set (roughly 7.4 GB on disk).
9.  Run the model. `python run_model.py` and `python run_model.py --sample` will run the model on the 1% sample. `python run_model.py --full` runs the model on the full data set.