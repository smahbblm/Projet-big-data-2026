import zipfile

with zipfile.ZipFile("Entrainement_ALS.zip", "r") as z:
    print(z.namelist())