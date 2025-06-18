from allocineAPI.allocineAPI import allocineAPI
import pandas as pd

api = allocineAPI()

# Récupération des horaires de cinéma pour Paris
cinemas = api.get_cinema("ville-115755")

# Transformation en DataFrame
df = pd.DataFrame(cinemas)
df_sorted = df.sort_values('name')

# Affichage du DataFrame
print(df_sorted)
