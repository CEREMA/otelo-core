from functools import cached_property
from pg.pgutils import PGScript
import environ

# Lecture .env
env = environ.Env()
environ.Env.read_env()


OTELO_DATA_DB = (
    env("OTELO_DB_HOST"),
    env("OTELO_DB_DATABASE"),
    env("OTELO_DB_PORT"),
    env("OTELO_DB_USER"),
    env("OTELO_DB_PASSWORD"),
)

CATEGORIES_HEBERGEMENT = {
    "1": "Aire Station Nomades",
    "2": "Autre Ctre.Accueil",
    "3": "C.A.D.A.",
    "4": "C.H.R.S.",
    "5": "C.P.H.",
    "6": "Foyer Jeunes Trav.",
    "7": "Foyer Trav. Migrants",
    "8": "Héberg.Fam.Malades",
    "9": "Log.Foyer non Spéc.",
    "A": "Maisons Relais-Pens.",
    "B": "Resid.Soc. hors MRel",
}


class Data:
    """
    Permet l'interrogation des données d'un pack régional
    """

    FEUILLES = [
        "f_synthese",
        "fb1_sa_rp",
        "fb1_fortune_rp",
        "fb1_hotel_rp",
        "fb1_sa_sne",
        "fb1_fortune_sne",
        "fb1_hotel_sne",
        "fb1_heberges_finess",
        "fb1_cohab_interg_filo",
        "fb1_heberges_sne",
        "fb1_inadeq_fin",
        "fb1_mv_qualite_rp",
        "fb1_mv_qualite_filo",
        "fb1_mv_qualite_ff",
        "fb1_inadeq_physique_rp",
        "fb1_inadeq_physique_filo",
        "fb1_parc_social_sne",
        "fb2_omphale",
        "fb2_flux_filo",
    ]

    def __init__(self, region, version=1):
        self.region = region
        self.version = version
        for feuille in self.FEUILLES:
            setattr(
                self, feuille, Feuille("r{0}_{1}".format(self.region, feuille), version)
            )

    def epcis(self):
        """
        Renvoie la liste des codes epcis présents dans le pack régional
        """
        return self.fb1_sa_rp.epcis

    def zos(self):
        """
        Renvoie la liste des codes ZO associés au pack régional
        """
        return self.fb1_sa_rp.zos

    def parc_total(self, code):
        return self.fb2_flux_filo.valeur("Parctot_17", code)

    def b1_sa_rp(self, code):
        return self.fb1_sa_rp.valeur("nb_pers", code)

    def b1_fortune_rp(self, code):
        return self.fb1_fortune_rp.valeur("fortune_pond", code)

    def b1_hotel_rp(self, code):
        return self.fb1_hotel_rp.valeur("Nb_menages", code)

    def b1_sa_sne(self, code):
        return self.fb1_sa_sne.valeur("nb_menages", code)

    def b1_fortune_sne(self, code):
        return self.fb1_fortune_sne.valeur(
            "nb_menages_camping", code
        ) + self.fb1_fortune_sne.valeur("nb_menages_squat", code)

    def b1_hotel_sne(self, code):
        return self.fb1_hotel_sne.valeur("nb_menages", code)

    def b1_hebergement_finess(self, code, type_hebergement):
        return self.fb1_heberges_finess.valeur_somme_colonnes(
            None, code, start_expression=CATEGORIES_HEBERGEMENT[str(type_hebergement)]
        )

    def b1_cohab_interg_filo(self, code):
        return self.fb1_cohab_interg_filo.valeur("nb_foyers_fiscaux", code)

    def b1_heberges_sne(self, code, type):
        return self.fb1_heberges_sne.valeur("nb_menages_" + type, code)

    def b1_inadeq_fin(self, code, taux, type):
        champ = "nb_all_plus{0}_{1}".format(str(taux), type)
        return self.fb1_inadeq_fin.valeur(champ, code)

    def b1_mv_qualite_rp(
        self, code, confort, rehabilitation=False, occupation="prop_loc"
    ):
        if not rehabilitation:
            if confort == "RP_abs_sani":
                filtre_col = []
                if "loc" in occupation:
                    filtre_col.append("sani_loc_nonHLM")
                if "prop" in occupation:
                    filtre_col.append("sani_ppT")
            elif confort == "RP_abs_sani_chauf":
                filtre_col = []
                if "loc" in occupation:
                    filtre_col.append("sani_chfl_loc_nonHLM")
                if "prop" in occupation:
                    filtre_col.append("sani_chfl_ppT")
        else:  ## n'est plus utilisé
            if confort == "RP_abs_sani":
                filtre_col = ["sani_ppT"]
            elif confort == "RP_abs_sani_chauf":
                filtre_col = ["sani_chfl_ppT"]
        return self.fb1_mv_qualite_rp.valeur_somme_colonnes(filtre_col, code)

    def b1_mv_qualite_filo(self, code, rehabilitation=False, occupation="prop_loc"):
        if not rehabilitation:
            value = 0
            if "loc" in occupation:
                value += self.fb1_mv_qualite_filo.valeur("pppi_lp", code)
            if "prop" in occupation:
                value += self.fb1_mv_qualite_filo.valeur("pppi_po", code)
            return value
        return self.fb1_mv_qualite_filo.valeur("pppi_po", code)

    def b1_mv_qualite_ff(
        self, code, confort, qualite, rehabilitation=False, occupation="prop_loc"
    ):
        value = 0
        variable = "pp_ss_"
        if qualite == "FF_Ind":
            variable += ""
        elif qualite == "FF_ss_ent":
            variable += "ent_"
        elif qualite == "FF_ss_ent_ mvq":
            variable += "quali_ent_"
        if confort == "FF_abs_wc":
            variable += "wc_"
        elif confort == "FF_abs_chauf":
            variable += "chauff_"
        elif confort == "FF_abs_sani":
            variable += "sdb_"
        elif confort == "FF_abs_wc_chauf":
            variable += "wc_chauff_"
        elif confort == "FF_abs_wc_sani":
            variable += "wc_sdb_"
        elif confort == "FF_abs_sani_chauf":
            variable += "sdb_chauff_"
        elif confort == "FF_abs_wc_sani_chauf":
            variable += "3elts_"
        if not rehabilitation:
            if "loc" in occupation:
                value += self.fb1_mv_qualite_ff.valeur(variable + "loc", code)
            if "prop" in occupation:
                value += self.fb1_mv_qualite_ff.valeur(variable + "ppt", code)
            return value
        else:
            return self.fb1_mv_qualite_ff.valeur(variable + "ppt", code)

    def b1_inadeq_physique_rp(self, code, type_proprio, type_surocc):
        return self.fb1_inadeq_physique_rp.valeur(
            "nb_men_" + type_surocc.lower() + "_" + type_proprio,
            code,
        )

    def b1_inadeq_physique_filo(self, code, type_proprio, type_surocc):
        surocc = "leg" if type_surocc == "Mod" else "lourde"
        val = self.fb1_inadeq_physique_filo.valeur(
            "surocc_" + surocc + "_" + type_proprio,
            code,
        )
        return val

    def b1_parc_social_sne(self, code, motif):
        variable = {
            "Tout": "crea",
            "Env": "crea_voisin",
            "Assis": "crea_mater",
            "Rappr": "crea_services",
            "Trois": "crea_motifs",
        }
        return self.fb1_parc_social_sne.valeur(variable[motif], code)

    def b2_omphale(self, code, scenario, periode):
        an_N0 = 2021
        if self.version == 1:
            an_N0 = 2017
        an_N1 = an_N0 + periode
        val_N1 = self.fb2_omphale.valeur_omphale(scenario, code, an_N1)
        val_N0 = self.fb2_omphale.valeur_omphale(scenario, code, an_N0)
        return round(val_N1 - val_N0)

    def epci_moins_50k(
        self,
        code,
    ):
        """
        Regarde s'il y a une clef de répartition definie pour l'EPCI
        Si oui, c'est qu'il s'agit d'un epci de moins de 50000 hab
        """
        cle = self.fb2_omphale.valeur_omphale("cle", code, 2020)
        if cle != 0:
            return True
        return False

    def taux_croissance_annuel_omphale(self, code, scenario, periode):
        an_N0 = 2021
        if self.version == 1:
            an_N0 = 2017
        an_N1 = an_N0 + periode
        val_N1 = self.fb2_omphale.valeur_omphale(scenario, code, an_N1)
        val_N0 = self.fb2_omphale.valeur_omphale(scenario, code, an_N0)
        if val_N0 == 0:
            return 0
        tx = ((val_N1 / val_N0) ** (1 / periode)) - 1.0
        return round(tx, 10)

    def b2_chronique_omphale(self, code, scenario, annee_finale):
        x = list(range(2017, annee_finale))
        y = []
        for an in x:
            y.append(self.fb2_omphale.valeur_omphale(scenario, code, an))
        return x, y

    def b2_taux_restruc_an(self, code):
        return (1.0 + self.fb2_flux_filo.taux("restruc", code)) ** (1.0 / 6.0) - 1.0

    def b2_taux_disp_an(self, code):
        return (1.0 + self.fb2_flux_filo.taux("disp", code)) ** (1.0 / 6.0) - 1.0

    def b2_taux_lv(self, code):
        return self.fb2_flux_filo.taux("lv", code)

    def b2_taux_rs(self, code):
        return self.fb2_flux_filo.taux("rs", code)

    def b2_taux_rp(self, code):
        return self.fb2_flux_filo.taux("rp", code)

    def evolution_menages(self, code):
        return round(self.f_synthese.valeur("evol_men_1217", code))

    def taux_evolution_annuel_menages(self, code):
        return round(self.f_synthese.valeur("tx_evol_men1217", code), 10)


class Feuille(PGScript):
    def __init__(self, nom_table, version=1):
        super().__init__(*OTELO_DATA_DB, log=False)
        if version == 1:
            self.schema = "public"
        elif version == 2:
            self.schema = "v2024"
        self.table = nom_table
        self.nom_table = self.schema + "." + nom_table
        self.nom_table_zo = self.schema + "." + nom_table + "_zo"

    @cached_property
    def df(self):
        """
        Renvoie un Dataframe de la feuille du pack de données (à l'EPCI)
        """
        order_by = ", annee" if "omphale" in self.nom_table else ""
        df = self.get_table_epci(self.nom_table, order_by)
        return df

    @cached_property
    def df_zo(self):
        """
        Renvoie un Dataframe de la feuille du pack de données agrégé à la ZO
        """
        order_by = ", annee" if "omphale" in self.nom_table else ""
        df_zo = self.get_table_zo(self.nom_table_zo, order_by)
        return df_zo

    @cached_property
    def epcis(self):
        """
        Renvoie la liste des EPCI de la feuille
        """
        toto = self.get_epcis(self.nom_table)
        return toto

    @cached_property
    def zos(self):
        """
        Renvoie la liste des ZO de la feuille
        """
        return self.get_zos(self.nom_table_zo)

    def valeur(self, nom_col, code):
        """
        Recupère la valeur de la colonne pour le code correspondant dans la base de données
        """
        try:
            if code in self.epcis:
                val = self.get_val_epci(nom_col, self.nom_table, code)
            else:
                val = self.get_val_zo(nom_col, self.nom_table_zo, code)
            return float(val)
        except ValueError as e:
            return 0.0
        except Exception as e:
            return 0.0

    def valeur_omphale(self, nom_col, code, annee):
        """
        Recupère la valeur de la colonne pour le code correspondant dans la table Omphale
        """
        try:
            if code in self.epcis:
                val = self.get_val_omphale_epci(nom_col, self.nom_table, code, annee)
            else:
                val = self.get_val_omphale_zo(nom_col, self.nom_table_zo, code, annee)
            return float(val)
        except ValueError as e:
            return 0.0
        except Exception as e:
            print("-", e)
            return 0.0

    def valeur_somme_colonnes(self, colonnes, code, start_expression=None):
        if not start_expression:
            champs_sum = [
                'COALESCE("' + champ[1] + '", 0) '
                for champ in self.lister_champs(self.schema, self.table)
                if champ[1] in colonnes
            ]
        else:
            champs_sum = [
                'COALESCE("' + champ[1] + '", 0) '
                for champ in self.lister_champs(self.schema, self.table)
                if champ[1].startswith(start_expression)
            ]
        nom_col = " + ".join(champs_sum)
        try:
            if code in self.epcis:
                val = self.get_expr_epci(nom_col, self.nom_table, code)
            else:
                val = self.get_expr_zo(nom_col, self.nom_table_zo, code)
            return float(val)
        except ValueError as e:
            return 0.0
        except Exception as e:
            return 0.0

    def taux(self, type, code):
        """
        Renvoie les taux restructuration, disparition, lv, rs pour l'EPCI ou la ZO
        Possible uniquement avec la Feuille issue de 2_2_Flux_Filo
        """
        if type not in ("restruc", "disp", "rp", "lv", "rs"):
            raise Exception(
                "L'attribut type prend les attributs suivants : 'restruc', 'disp', 'rp', 'lv' ou 'rs'"
            )
        if type == "restruc":
            return self.valeur("txRest_parctot_1117", code)
        elif type == "disp":
            return self.valeur("txDisp_parctot_1117", code)
        elif type == "rp":
            return self.valeur("txRP_parctot17", code)
        elif type == "lv":
            return self.valeur("txLV_parctot17", code)
        elif type == "rs":
            return self.valeur("txRS_parctot17", code)
        else:
            return None
