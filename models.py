from dataclasses import dataclass, field
from typing import List, ClassVar
from typing import Optional


@dataclass
class Hebergement:
    code: str
    nom: str


@dataclass
class ZoneOtelo:
    code: str
    code_region: str
    version: int = 1
    parametre: "Parametres" = None


@dataclass
class EPCI:
    code: str
    code_region: str
    parametre: "Parametres" = None
    custom_param: "CustomParam" = None
    version: int = 1
    zo: "ZoneOtelo" = None


@dataclass
class Parametres:
    nom: str
    b1_horizon_resorption: int = 20
    b11_sa: bool = True
    b11_fortune: bool = True
    b11_hotel: bool = True
    source_b11: str = "RP"
    b11_etablissement: List["Hebergement"] = field(default_factory=list)
    b11_part_etablissement: int = 100
    b12_cohab_interg_subie: int = 50
    b12_heberg_particulier: bool = True
    b12_heberg_gratuit: bool = True
    b12_heberg_temporaire: bool = True
    b13_taux_effort: int = 30
    b13_acc: bool = True
    b13_plp: bool = True
    b13_taux_reallocation: int = 80
    source_b14: str = "RP"
    b14_confort: str = "RP_abs_sani"
    b14_qualite: str = ""
    b14_occupation: str = "prop_loc"
    b14_taux_reallocation: int = 80
    source_b15: str = "RP"
    b15_surocc: str = "Acc"
    b15_proprietaire: bool = False
    b15_loc_hors_hlm: bool = True
    b15_taux_reallocation: int = 80
    b17_motif: str = "Tout"
    b2_scenario_omphale: str = "Central_C"
    b2_tx_restructuration: float = 0.0
    b2_tx_disparition: float = 0.0
    b2_tx_vacance: float = 0.0
    b2_tx_rs: float = 0.0

    def __str__(self):
        return self.nom

    @property
    def hebergements_display(self) -> str:
        noms = [str(h) for h in self.b11_etablissement]
        return " - ".join(noms)


@dataclass
class CustomParam:
    b2_evol_demo_an: Optional[int] = None
    b2_tx_restructuration_custom: Optional[float] = None
    b2_tx_disparition_custom: Optional[float] = None
    b2_tx_rs_custom: Optional[float] = None
    b2_tx_lv_custom: Optional[float] = None
