# %% Imports
import kaggle

# %% Download
kaggle.api.authenticate()

try:
    kaggle.api.dataset_download_files('xavier4t/sports-and-restaurants', path=r"./data/raw", unzip=True)
    print("Data downloaded!")
except Exception as e:
    print("Something went wrong!")
    print(e.message)
    