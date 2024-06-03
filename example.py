from chargement import CATEGORIES_HEBERGEMENT
from models import EPCI, ZoneOtelo, Parametres, Hebergement
from resultat import EPCIResultat, ZOResultat

VERSION = 2

# Définition d'un paramètrage
# Les paramètres par défaut sont détaillés dans models.Parametres
param_standard = Parametres(
    nom="Standard",
    b11_etablissement=[
        Hebergement(code, nom) for code, nom in CATEGORIES_HEBERGEMENT.items()
    ],
)

# période de projection en année
PERIODE_PROJECTION = 6

# CALCUL POUR UN BASSIN D'HABITAT
code_zo = "REG24_ZO28_zone_1"
zo = ZoneOtelo(code_zo, "24", parametre=param_standard, version=VERSION)
resultat_zo = ZOResultat(zo, periode_projection=PERIODE_PROJECTION)
print(f"ZO {code_zo}")
print("-" * 20)
print(f"Besoin en stock sur {PERIODE_PROJECTION} ans :", resultat_zo.besoin_en_stock())
print(
    f"Besoin en flux sur {PERIODE_PROJECTION} ans :", resultat_zo.demande_potentielle()
)
print(f"Besoin total sur {PERIODE_PROJECTION} ans :", resultat_zo.besoin_total(), "\n")


# CALCUL POUR UN EPCI
code_epci = "200090751"
print(f"EPCI {code_epci}")
print("-" * 20)
epci = EPCI(code_epci, "32", parametre=param_standard, version=VERSION)
resultat = EPCIResultat(epci, periode_projection=6)
print(f"Besoin en stock sur {PERIODE_PROJECTION} ans :", resultat.besoin_en_stock())
print(f"Besoin en flux sur {PERIODE_PROJECTION} ans :", resultat.demande_potentielle())
print(f"Besoin total sur {PERIODE_PROJECTION} ans :", resultat.besoin_total())
