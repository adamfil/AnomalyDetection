# AnomalyDetection

ECCC ML QA
==============================
To begin, clone this repository onto your local machine.

Data Extraction
------------
If you are using data which is already extracted and in XML format, copy it into 'data/raw/eccl_ml_qa/all_stations/deploy/'. If instead you would like to extract novel data for training and testing, follow below:
1. Obtain Arkeon credentials in order to connect to the archive 
2. Using SQL Developer, connect to Arkeon database 
3. If you would like to extract only anomalous data (for the purpose of metrics generation), paste the .sql script from /SQL/anomalies_getter.sql into SQL Developer while connected to Arkeon. If you would like to extract both anomalous and non-anomalous data (for the purpose of training and testing), paste the .sql script from /SQL/historic_getter.sql
4. Run .sql statement script 
5. After SQL Developer returns sample rows, right click on table and click export
6. Change file amount from "one file" to "separate files" and change file type to loader or .ldr files 
7. Change export path to 'data/raw/eccl_ml_qa/all_stations/deploy/' 
8. Export data to given path

You will now have the necessary XML files in 'data/raw/eccl_ml_qa/all_stations/deploy/' to proceed.

Metrics 
------------
This section is aimed at running Metrics.ipynb. Running this will provide you with metrics for the given data, as well as dataframes which can be used to train machine learning algorithms on.

The script for the modelling is based on following steps:
1. Create conda environment with all necessary packages installed based on environment.yml. Only necessary at the first time: \
 `conda env create -f environment.yml`
2. Activate the conda environment: \
`conda activate eccc_ml_qa`
3. Confirm data has been extracted to 'data/raw/eccl_ml_qa/all_stations/deploy/' 
    - Otherwise the path has to be changed at two places in the code: 
        1. 'path' variable at the end of xml2dict.py
        2. Path hardcoded in the function 'main_folder' in dict2tabular.py
4. Change directory into the folder of the data related source files (commands might differ under Windows) \
`cd src/data`
4. Extract relevant fields from XML into dictionaries per station \
`python xml2dict.py`
5. Transform the dictionaries into DataFrames which the notebook notebooks take as input: \
`python dict2tabular.py`
6. Execute the modeling steps in the notebook: 
    - Start the jupyter notebook server: 
        1. Locally: `jupyter notebook`
        2. Remote: [Link](https://medium.com/@apbetahouse45/how-to-run-jupyter-notebooks-on-remote-server-part-1-ssh-a2be0232c533) to article with description.
    - Open the file 'notebooks/Metrics.ipynb' in your browser and executed the steps chronologically (Shift+Return)

Training and Testing
------------
Prerequisites: The notebook assumes that steps 1-6 from above (Metrics) have been executed.
1. Execute the testing steps in the notebook: 
    - Start the jupyter notebook server: 
        1. Locally: `jupyter notebook`
        2. Remote: [Link](https://medium.com/@apbetahouse45/how-to-run-jupyter-notebooks-on-remote-server-part-1-ssh-a2be0232c533) to article with description.
    - Open the file 'notebooks/Train_and_test.ipynb' in your browser and executed the steps chronologically (Shift+Return)

Live Detection
------------
Prerequisites: The notebook assumes that the above (Training and Testing) has been executed.
1. Execute the testing steps in the notebook: 
    - Start the jupyter notebook server: 
        1. Locally: `jupyter notebook`
        2. Remote: [Link](https://medium.com/@apbetahouse45/how-to-run-jupyter-notebooks-on-remote-server-part-1-ssh-a2be0232c533) to article with description.
    - Open the file 'notebooks/Detection.ipynb' in your browser and executed the steps chronologically (Shift+Return)

Project Organization
------------

    ├── README.md          <- The top-level README for developers using this project.
    ├── data
    │   ├── interim        <- Intermediate data that has been transformed.
    │   ├── processed      <- Tabular data for modeling.
    │   └── raw            <- The original, immutable data dump.
    │
    ├── sql       
    │   ├── anomalies_getter.sql   <- Script to get only anomalous data from Arkeon database
    │   └── historic_getter.sql    <- Script to get all data from Arkeon database
    │
    ├── notebooks          
    │   ├── TrainingTemplate.ipynb
    │   └── TestingTemplate.ipynb
    │
    ├── src               
    │   └── data           <- Scripts preprocessing the data from XML 
    │       ├── xml2dict.py   
    │       ├── dict2tabular.py
    │       └── test_dict2tabular.py
    │   
    └── environment.yml   <- The YAML file defining the conda environment (update using `make freeze`)


--------

