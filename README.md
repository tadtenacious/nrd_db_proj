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
   $ git clone https://github.com/tadtenacious/nrd_db_proj.git
   ```
2. Create a virtual environment with the required libraries:
    ```
    $ conda env create --name nrd -f=env.yml
    ```
3. Store the 3 csv files from HCUP in the data directory.
4. Activate the virtual environment:
   ```
    $ conda activate nrd
   ```
5. Create the configuration file for connecting to the database.
   ```
   (nrd) $ python nrd.py --config
   ```
6. Run the tests. This is done from the parent directory, so no need to cd into `tests`.
   ```
   python -m pytest
   ```
7. Load the data to the database and perform most of the feature engineering. The full and sample data sets will also be extracted. The full data set is roughly 7.4 GB on disk. 
   ```
   (nrd) $ python nrd.py --etl
   ```

8.  Run the model on the sample. 
    ```
    (nrd) $ python nrd.py --model sample
    ```
9. Run the model on the full data set. This was only successfully run on a computer with 125 GB of RAM.
    ```
    (nrd) $ python nrd.py --model full
    ```