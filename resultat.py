from functools import cached_property

from chargement import Data


class Resultat:
    """

    Calcul des résultats au Bassin d'habitat ou à l'EPCI

    """

    def __init__(self, periode_projection=6):
        self.code = None  # sera défini dans les classes filles
        self.code_region = None  # idem
        self.version = None  # idem
        self.parametre = None  # idem
        self.custom_parametre = None  # idem
        self.periode_projection = periode_projection

    @cached_property
    def data(self):
        return Data(self.code_region, self.version)

    def coeff(self, projection=True):
        if projection:
            horizon_resorption = self.parametre.b1_horizon_resorption
            if self.periode_projection < horizon_resorption:
                return self.periode_projection / horizon_resorption
            else:
                return 1
        return 1

    @property
    def ratio_4_3(self):
        p = self.parametre
        if p.b13_taux_effort < 35:
            if self.code_region == "11":  # POUR ZO IDF
                return 0.0257
            else:
                return 0.0521
        else:
            if self.code_region == "11":  # POUR ZO IDF
                return 0.017
            else:
                return 0.0441

    @property
    def ratio_2_5(self):
        if self.code_region == "11":  # POUR ZO IDF
            return 0.1729
        else:
            return 0.0025

    @property
    def ratio_3_5(self):
        p = self.parametre
        if p.b13_taux_effort < 35:
            if self.code_region == "11":  # POUR ZO IDF
                return 0.077
            else:
                return 0.0101
        else:
            if self.code_region == "11":  ### POUR ZO IDF
                return 0.0753
            else:
                return 0.0142

    @property
    def ratio_4_5(self):
        if self.code_region == "11":  # POUR ZO IDF
            return 0.1001
        else:
            return 0.0116

    def besoin_total(self, projection=True):
        return round(self.besoin_en_stock(projection) + self.demande_potentielle())

    def besoin_total_custom(self, projection=True):
        return round(
            self.besoin_en_stock(projection) + self.demande_potentielle_custom()
        )

    def besoin_en_stock(self, projection=True):
        return round(
            self.b11(projection)
            + self.b12(projection)
            + self.b13(projection)
            + self.b14(projection)
            + self.b15(projection)
            + self.b17(projection)
        )

    def b11(self, projection=True):
        p = self.parametre
        d = self.data
        resultat = 0
        if p.source_b11 == "RP":
            if p.b11_sa:
                resultat += d.b1_sa_rp(self.code)
            if p.b11_fortune:
                resultat += d.b1_fortune_rp(self.code)
            if p.b11_hotel:
                resultat += d.b1_hotel_rp(self.code)
        elif p.source_b11 == "SNE":
            if p.b11_sa:
                resultat += d.b1_sa_sne(self.code)
            if p.b11_fortune:
                resultat += d.b1_fortune_sne(self.code)
            if p.b11_hotel:
                resultat += d.b1_hotel_sne(self.code)
        for hebergement in p.b11_etablissement:
            resultat += (p.b11_part_etablissement / 100) * d.b1_hebergement_finess(
                self.code, hebergement.code
            )
        return round(resultat * self.coeff(projection))

    def b12(self, projection=True):
        p = self.parametre
        d = self.data
        resultat = (p.b12_cohab_interg_subie / 100) * d.b1_cohab_interg_filo(self.code)
        if p.b12_heberg_particulier:
            resultat += d.b1_heberges_sne(self.code, "particulier")
        if p.b12_heberg_gratuit:
            resultat += d.b1_heberges_sne(self.code, "gratuit")
        if p.b12_heberg_temporaire:
            resultat += d.b1_heberges_sne(self.code, "temp")
        return round(resultat * self.coeff(projection))

    def b13(self, projection=True, correction=True, reallocation=True):
        p = self.parametre
        d = self.data
        resultat = 0
        if p.b13_acc:
            resultat += d.b1_inadeq_fin(self.code, p.b13_taux_effort, "Acc")
        if p.b13_plp:
            resultat += d.b1_inadeq_fin(self.code, p.b13_taux_effort, "PLP")
        if correction:
            resultat += -1 * self.ratio_4_3 * self.b14(projection=False)
        if reallocation:
            resultat = (1 - p.b13_taux_reallocation / 100.0) * resultat
        return round(resultat * self.coeff(projection))

    def b14(self, projection=True, reallocation=True):
        p = self.parametre
        d = self.data
        resultat = 0
        if p.source_b14 == "RP":
            resultat = d.b1_mv_qualite_rp(
                self.code, p.b14_confort, occupation=p.b14_occupation
            )
        elif p.source_b14 == "Filo":
            resultat = d.b1_mv_qualite_filo(self.code, occupation=p.b14_occupation)
        elif p.source_b14 == "FF":
            resultat = d.b1_mv_qualite_ff(
                self.code, p.b14_confort, p.b14_qualite, occupation=p.b14_occupation
            )
        if reallocation:
            resultat = (1 - p.b14_taux_reallocation / 100.0) * resultat
        return round(resultat * self.coeff(projection))

    def besoin_en_rehabilitation(self, projection=True):
        p = self.parametre
        d = self.data
        resultat = 0
        if p.source_b14 == "RP":
            resultat = d.b1_mv_qualite_rp(self.code, p.b14_confort, rehabilitation=True)
        elif p.source_b14 == "Filo":
            resultat = d.b1_mv_qualite_filo(self.code, rehabilitation=True)
        elif p.source_b14 == "FF":
            resultat = d.b1_mv_qualite_ff(
                self.code, p.b14_confort, p.b14_qualite, rehabilitation=True
            )
        return round(resultat * self.coeff(projection))

    def b15(self, projection=True, correction=True, reallocation=True):
        p = self.parametre
        d = self.data
        resultat = 0
        if p.source_b15 == "RP":
            if p.b15_proprietaire:
                resultat += d.b1_inadeq_physique_rp(self.code, "ppT", p.b15_surocc)
            if p.b15_loc_hors_hlm:
                resultat += d.b1_inadeq_physique_rp(
                    self.code, "loc_nonHLM", p.b15_surocc
                )
        elif p.source_b15 == "Filo":
            if p.b15_proprietaire:
                resultat += d.b1_inadeq_physique_filo(self.code, "po", p.b15_surocc)
            if p.b15_loc_hors_hlm:
                resultat += d.b1_inadeq_physique_filo(self.code, "lp", p.b15_surocc)
        if correction:
            resultat += (
                -1 * self.ratio_2_5 * self.b12(projection=False)
                - self.ratio_3_5 * self.b13(projection=False)
                - self.ratio_4_5 * self.b14(projection=False)
            )
        if reallocation:
            resultat = (1 - p.b15_taux_reallocation / 100.0) * resultat
        return round(resultat * self.coeff(projection))

    def b17(self, projection=True):
        p = self.parametre
        d = self.data
        resultat = d.b1_parc_social_sne(self.code, p.b17_motif)
        return round(resultat * self.coeff(projection))

    def parc_total_actuel(self):
        d = self.data
        return d.parc_total(self.code)

    def parc_rp_actuel(self):
        d = self.data
        tx_rp_actuel = d.b2_taux_rp(self.code)
        return round(self.parc_total_actuel() * tx_rp_actuel)

    def taux_croissance_annuel(self):
        p = self.parametre
        d = self.data
        return d.taux_croissance_annuel_omphale(
            self.code, p.b2_scenario_omphale, self.periode_projection
        )

    def demande_potentielle(self):
        p = self.parametre
        d = self.data
        resultat = ((self.parc_rp_actuel() + self.b21()) / self.taux_rp) - (
            self.parc_total_actuel() - self.besoin_renouvellement()
        )
        return round(resultat)

    def demande_potentielle_custom(self):
        p = self.parametre
        d = self.data
        resultat = (
            (self.parc_rp_actuel() + self.b21_custom()) / self.taux_rp_custom
        ) - (self.parc_total_actuel() - self.besoin_renouvellement_custom())
        return round(resultat)

    def b21(self):
        p = self.parametre
        d = self.data
        return round(
            d.b2_omphale(self.code, p.b2_scenario_omphale, self.periode_projection)
        )

    def b21_custom(self):
        p = self.custom_parametre
        if p:
            if p.b2_evol_demo_an:
                return p.b2_evol_demo_an * self.periode_projection
        return self.b21()

    def b22(self):
        return round(self.demande_potentielle() - self.b21())

    def b22_custom(
        self,
    ):
        return round(self.demande_potentielle_custom() - self.b21_custom())

    def besoin_renouvellement(self):
        p = self.parametre
        d = self.data
        renouvellement = self.parc_total_actuel() * (
            self.taux_restructuration - self.taux_disparition
        )
        return round(-1.0 * renouvellement)

    def besoin_renouvellement_custom(self):
        p = self.parametre
        d = self.data
        renouvellement = self.parc_total_actuel() * (
            self.taux_restructuration_custom - self.taux_disparition_custom
        )
        return round(-1.0 * renouvellement)

    def evolution_nb_lv(self):
        p = self.parametre
        d = self.data
        evol = (
            d.parc_total(self.code)
            - self.besoin_renouvellement()
            + self.demande_potentielle()
        ) * self.taux_lv - d.parc_total(self.code) * d.b2_taux_lv(self.code)
        return round(evol)

    def evolution_nb_lv_custom(self):
        p = self.parametre
        d = self.data
        evol = (
            d.parc_total(self.code)
            - self.besoin_renouvellement_custom()
            + self.demande_potentielle_custom()
        ) * self.taux_lv_custom - d.parc_total(self.code) * d.b2_taux_lv(self.code)
        return round(evol)

    def evolution_nb_rs(self):
        p = self.parametre
        d = self.data
        evol = (
            d.parc_total(self.code)
            - self.besoin_renouvellement()
            + self.demande_potentielle()
        ) * self.taux_rs - d.parc_total(self.code) * d.b2_taux_rs(self.code)
        return round(evol)

    def evolution_nb_rs_custom(self):
        p = self.parametre
        d = self.data
        evol = (
            d.parc_total(self.code)
            - self.besoin_renouvellement_custom()
            + self.demande_potentielle_custom()
        ) * self.taux_rs_custom - d.parc_total(self.code) * d.b2_taux_rs(self.code)
        return round(evol)

    @property
    def taux_restructuration_an(self):
        p = self.parametre
        d = self.data
        tx_actuel = d.b2_taux_restruc_an(self.code)
        return round(tx_actuel + p.b2_tx_restructuration / 100.0, 10)

    @property
    def taux_restructuration(self):
        return round(
            (1.0 + self.taux_restructuration_an) ** (self.periode_projection) - 1.0, 10
        )

    @property
    def taux_restructuration_custom_an(self):
        p = self.custom_parametre
        if p:
            if p.b2_tx_restructuration_custom is not None:
                return round(p.b2_tx_restructuration_custom / 100.0, 10)
        return self.taux_restructuration_an

    @property
    def taux_restructuration_custom(self):
        return round(
            (1.0 + self.taux_restructuration_custom_an) ** (self.periode_projection)
            - 1.0,
            10,
        )

    @property
    def taux_disparition_an(self):
        p = self.parametre
        d = self.data
        tx_actuel = d.b2_taux_disp_an(self.code)
        return round(tx_actuel + p.b2_tx_disparition / 100.0, 10)

    @property
    def taux_disparition(self):
        return round(
            (1.0 + self.taux_disparition_an) ** (self.periode_projection) - 1.0, 10
        )

    @property
    def taux_disparition_custom_an(self):
        p = self.custom_parametre
        if p:
            if p.b2_tx_disparition_custom is not None:
                return round(p.b2_tx_disparition_custom / 100.0, 10)
        return self.taux_disparition_an

    @property
    def taux_disparition_custom(self):
        return round(
            (1.0 + self.taux_disparition_custom_an) ** (self.periode_projection) - 1.0,
            10,
        )

    @property
    def taux_rp(self):
        return 1.0 - self.taux_lv - self.taux_rs

    @property
    def taux_rp_custom(self):
        return 1.0 - self.taux_lv_custom - self.taux_rs_custom

    @property
    def taux_lv(self):
        p = self.parametre
        d = self.data
        tx_actuel = d.b2_taux_lv(self.code)
        return round(tx_actuel + p.b2_tx_vacance / 100.0, 10)

    @property
    def taux_lv_custom(self):
        p = self.custom_parametre
        if p:
            if p.b2_tx_lv_custom is not None:
                return round(p.b2_tx_lv_custom / 100.0, 10)
        return self.taux_lv

    @property
    def taux_rs(self):
        p = self.parametre
        d = self.data
        tx_actuel = d.b2_taux_rs(self.code)
        return round(tx_actuel + p.b2_tx_rs / 100.0, 10)

    @property
    def taux_rs_custom(self):
        p = self.custom_parametre
        if p:
            if p.b2_tx_rs_custom is not None:
                return round(p.b2_tx_rs_custom / 100.0, 10)
        return self.taux_rs


class EPCIResultat(Resultat):
    def __init__(self, epci, periode_projection=6):
        super().__init__(periode_projection=periode_projection)
        self.code = epci.code
        self.version = epci.version
        self.code_region = epci.code_region
        self.parametre = epci.parametre
        self.custom_parametre = epci.custom_param


class ZOResultat(Resultat):
    def __init__(self, zo, periode_projection=6):
        super().__init__(periode_projection=periode_projection)
        self.code = zo.code
        self.version = zo.version
        self.code_region = zo.code_region
        self.parametre = zo.parametre
        self.custom_parametre = None
