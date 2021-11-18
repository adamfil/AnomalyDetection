# AnomalyDetection

ECCC ML QA
==============================

Modeling
------------

The script for the modelling is based on following steps:
1. Create conda environment with all necessary packages installed based on environment.yml. Only necessary at the first time: \
 `conda env create -f environment.yml`
2. Activate the conda environment: \
`conda activate eccc_ml_qa`
3. Copy xml data into 'data/raw/eccl_ml_qa/all_stations/deploy/' 
    - Otherwise the path has to be changed at two places in the code: 
        1. 'path' variable at the end of xml2dict.py
        2. Path hardcoded in the function 'main_folder' in dict2tabular.py
4. Change directory into the folder of the data related source files (commands might differ under Windows) \
`cd src/data`
4. Extract relevant fields from XML into dictionaries per station \
`python xml2dict.py`
5. Transform the dictionaries into DataFrames which the notebook '02-CD-Modeling' takes as input: \
`python dict2tabular.py`
6. Execute the modeling steps in the notebook: 
    - Start the jupyter notebook server: 
        1. Locally: `jupyter notebook`
        2. Remote: [Link](https://medium.com/@apbetahouse45/how-to-run-jupyter-notebooks-on-remote-server-part-1-ssh-a2be0232c533) to article with description.
    - Open the file 'notebooks/TestingTemplate.ipynb' in your browser and executed the steps chronologically (Shift+Return)

Statistics and Visualizations
------------


 
Prerequisites: The notebook assumes that steps 1-4 from above have been executed.

Project Organization
------------

    ├── README.md          <- The top-level README for developers using this project.
    ├── data
    │   ├── interim        <- Intermediate data that has been transformed.
    │   ├── processed      <- Tabular data for modeling.
    │   └── raw            <- The original, immutable data dump.
    │    
    ├── models             <- Trained and serialized models, model predictions, or model summaries
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

