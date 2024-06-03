import csv
import datetime
import inspect
import logging
import os
import time
import types
from random import randint
from logging.handlers import RotatingFileHandler

import psycopg2
import pandas as pd


## DECORATEURS
def change_args(nom_requete_sql):
    """
    Décorateur applicable pour modifier les paramètres à renvoyer
    à la méthode associée à une requete SQL
    nom_requete_sql : nom de la méthode associée à la requete SQL
    La méthode décorée doit renvoyer les valeurs modifiées sous forme de tuple
    """

    def decorateur(f):
        def interne(self, *args, **kwargs):
            args = f(self, *args, **kwargs)
            methode_sql = getattr(self, nom_requete_sql.lower())
            resultat = methode_sql(*args, **kwargs)
            return resultat

        return interne

    return decorateur


class PGScript:

    SCRIPTFILE = None
    LOGFILE = None

    def __init__(
        self,
        hote=None,
        base=None,
        port=None,
        utilisateur=None,
        motdepasse=None,
        log=True,
    ):
        """
        Constructeur
        Se connecte directement à la base de données
        """
        self.conn = Connexion(hote, base, port, utilisateur, motdepasse)
        self.log = log
        self.max_tentative = 3
        ## dicts pour enregistrer toutes les requetes SQL et les éventuels formats imposés à certaines requetes
        self.requete_sql, self.format_requete_sql = self._chargement_requetes()

    @classmethod
    def fichier_script(cls):
        """
        Retourne le fichier de script sql pour la classe
        """
        if cls.SCRIPTFILE is None:
            repertoire_module = os.path.dirname(os.path.abspath(inspect.getfile(cls)))
            fichier_script = os.path.join(
                repertoire_module, cls.__name__.lower() + ".sql"
            )
            return fichier_script
        if not os.path.exists(cls.SCRIPTFILE):
            raise FileNotFoundError("Fichier SCRIPTFILE inconnu")
        return cls.SCRIPTFILE

    def _chargement_requetes(
        self,
    ):
        """
        Charge la variable requete_sql avec toutes les requetes SQL
        de sa classe et des classes éventuelles dont elle hérite
        """
        requetes = {}
        format_requetes = {}
        for classe in inspect.getmro(self.__class__):
            if classe != type(object()):
                try:
                    fichier_script_classe = classe.fichier_script()
                    self._charger_requete_sql_depuis(
                        fichier_script_classe, requetes, format_requetes
                    )
                except (OSError, IOError, FileNotFoundError) as e:
                    print(e)
        return requetes, format_requetes

    def _charger_requete_sql_depuis(self, fichier, requetes, format_requetes):
        """
        Lit un fichier de requetes et intègre les requetes dans requete_sql
        """
        with open(fichier, "rt", encoding="UTF-8") as f:
            lignes = f.readlines()
            clef = None
            for ligne in lignes:
                if ligne.rstrip():  # la ligne n'est pas vide
                    if ligne.strip().startswith("##"):
                        clef = ligne.strip()[2:].strip().lower()
                        if "::" in clef:
                            clef, format = clef.split("::")
                            if format in ("list", "smart", "normal", "df"):
                                format_requetes[clef] = format
                        requetes[clef] = ""
                        continue
                    requetes[clef] += ligne

    @classmethod
    def fichier_log(cls):
        if cls.LOGFILE is None:
            repertoire_log = os.path.dirname(os.path.abspath(inspect.getfile(cls)))
            fichier_log = os.path.join(repertoire_log, cls.__name__ + "_LOG.sql")
            return fichier_log
        return cls.LOGFILE

    @classmethod
    def _changement_filehandler(cls, logger):
        for hdlr in logger.handlers:
            if isinstance(hdlr, RotatingFileHandler):
                logger.removeHandler(hdlr)
        file_formatter = logging.Formatter(
            "%(message)s-- Fin execution : %(asctime)s --\n"
        )
        # un fichier en mode 'append', avec 1 backup et une taille max de 100Mo
        file_handler = RotatingFileHandler(
            cls.fichier_log(), "w", 100000000, 1, encoding="utf-8"
        )
        file_handler.setLevel(logging.INFO)
        file_handler.setFormatter(file_formatter)
        logger.addHandler(file_handler)

    @classmethod
    def get_logger(cls):
        logger = logging.getLogger(cls.__name__)
        if not len(logger.handlers):
            logger.setLevel(logging.DEBUG)
            # creation d'un file handler qui envoie sur le fichier LOGFILE de la classe
            cls._changement_filehandler(logger)
            # création d'un second handler qui va rediriger chaque écriture de log sur la console
            steam_formatter = logging.Formatter("%(message)s")
            steam_handler = logging.StreamHandler()
            steam_handler.setLevel(logging.DEBUG)
            steam_handler.setFormatter(steam_formatter)
            logger.addHandler(steam_handler)
        return logger

    @classmethod
    def change_log(cls, nveau_log):
        cls.LOGFILE = nveau_log
        cls._changement_filehandler(cls.get_logger())

    def __getattr__(self, nom):
        if nom in self.requete_sql or nom == "execution":

            def fonction(self, *args, **kwargs):
                # on récupère les options eventuelles, sinon on applique le format imposé défini, sinon les paramètres par défaut de l'objet
                format = kwargs.get("format", self.format_requete_sql.get(nom, "smart"))
                csvfile = kwargs.get("export_csv", None)
                xlsfile = kwargs.get("export_xls", None)
                htmlfile = kwargs.get("export_html", None)
                max_tentative = kwargs.get("max_tentative", self.max_tentative)
                no_log = kwargs.get("no_log", False)
                log = self.log and not no_log
                if nom != "execution":
                    sql = self.requete_sql[nom].format(*args)
                else:
                    sql = args[0]
                resultat = self.executer_requete(sql, max_tentative=3, log=log)
                if resultat is None:
                    return False, -1
                if csvfile:
                    resultat.to_csv(csvfile)
                if xlsfile:
                    resultat.to_xls(xlsfile)
                if htmlfile:
                    resultat.to_html(htmlfile)
                if format == "smart":
                    return resultat.smart_data
                elif format == "normal":
                    return resultat.data
                elif format == "list":
                    return resultat.list_data
                elif format == "df":
                    return resultat.dataframe
                return resultat

            methode = types.MethodType(fonction, self)
            return methode
        else:
            raise AttributeError("Méthode non définie")

    def executer_requete(self, sql, max_tentative=3, log=True):
        tentative = 1
        while tentative <= max_tentative:
            try:
                start_time = time.time()
                resultat = self.conn.executer(sql)
                exec_time = time.time() - start_time
                if log:
                    logger = self.get_logger()
                    logger.info(sql)
                    logger.debug(
                        "-- Execution en " + str(datetime.timedelta(seconds=exec_time))
                    )
                return resultat
            except Exception as e:
                print(e)
                print("Tentative " + str(tentative) + " échouée...")
                if tentative == max_tentative:
                    if log:
                        logger = self.get_logger()
                        logger.error("REQUETE EN ECHEC : \n" + str(e) + "\n" + sql)
                tentative += 1
        return None

    def effacer_schemas(self, schemas):
        """
        Supprime les schemas de la base de données en CASCADE.
        """
        for schema in schemas:
            reussite, nb = self.effacer_schema(schema)
            if not reussite:
                return False, -1
        return True, len(schemas)

    def effacer_schemas_commencant_par(self, prefixe):
        """
        Supprime les schemas commencant par le préfixe spécifié de la base de données en CASCADE.
        """
        schemas = self.lister_schemas_commencant_par(prefixe)
        return self.effacer_schemas(schemas)

    def effacer_et_creer_schema(self, schema):
        """
        Supprime le schéma s'il existe et le recrée.
        """
        reussite, nb = self.effacer_schema(schema)
        if reussite:
            return self.creer_schema(schema)

    def effacer_et_creer_schemas(self, schemas):
        """
        Supprime et recrée les schémas.
        """
        for schema in schemas:
            reussite, nb = self.effacer_et_creer_schema(schema)
            if not reussite:
                return False, -1
        return True, len(schemas)

    def effacer_tables(self, schema, tables):
        """
        Supprime les tables listées du schéma.
        """
        for table in tables:
            reussite, nb = self.effacer_table(schema, table)
            if not reussite:
                return False, -1
        return True, len(tables)

    def effacer_tables_commencant_par(self, schema, prefixe):
        """
        Supprime les tables du schéma commençant par le prefixe specifié.
        """
        tables = self.lister_tables_commencant_par(schema, prefixe)
        return self.effacer_tables(schema, tables)

    @change_args("_ajouter_clef_primaire")
    def ajouter_clef_primaire(self, schema, table, champs):
        """
        Ajoute une clef primaire à la table composée de l'ensemble des champs présents dans la liste.
        """
        return schema, table, ", ".join(champs)

    @change_args("_ajouter_contrainte_unicite")
    def ajouter_contrainte_unicite(self, schema, table, champs):
        """
        Ajoute une contrainte d'unicité à la table composée de l'ensemble des champs présents dans la liste.
        """
        return schema, table, ", ".join(champs)

    @change_args("_ajouter_commentaire_sur_champ")
    def ajouter_commentaire_sur_champ(self, schema, table, champ, commentaire):
        """
        Ajout un commentaire au champ de la table.
        Renvoie True si le commentaire a bien été créé.
        """
        return schema, table, champ, commentaire.replace("'", "''")

    @change_args("_ajouter_commentaire_sur_table")
    def ajouter_commentaire_sur_table(self, schema, table, commentaire):
        """
        Ajout un commentaire à la table.
        Renvoie True si le commentaire a bien été créé.
        """
        return schema, table, commentaire.replace("'", "''")

    def mettre_en_place_serveur_distant_fdw(
        self,
        hote_distant,
        base_distante,
        port,
        utilisateur,
        motdepasse,
        nom_serveur_distant,
    ):
        """
        Prépare un serveur distant fdw et crée un user mapping associé.
        """
        valid, nb = self.creer_extension_fdw()
        valid2, nb = self.effacer_serveur_distant_fdw(nom_serveur_distant)
        valid3, nb = self.creer_serveur_distant_fdw(
            hote_distant, base_distante, port, nom_serveur_distant
        )
        valid4, nb = self.effacer_user_mapping_pour_serveur_distant_fdw(
            utilisateur, nom_serveur_distant
        )
        valid5, nb = self.creer_user_mapping_pour_serveur_distant_fdw(
            utilisateur, nom_serveur_distant, motdepasse
        )
        return (
            (
                True,
                1,
            )
            if valid and valid2 and valid3 and valid4 and valid5
            else (
                False,
                -1,
            )
        )

    @change_args("_creer_table_etrangere")
    def creer_table_etrangere(
        self, schema, table, schema_distant, table_distante, champs, nom_serveur
    ):
        """
        Efface puis crée une table étrangère à partir de la table distante
        la variable champ doit avoir le formalisme prévue par le renvoi de la méthode lister_champs()
        """
        self.effacer_table_etrangere(schema, table)
        champs_format = ",\n".join([c[1] + " " + c[2] for c in champs])
        return schema, table, schema_distant, table_distante, champs_format, nom_serveur

    def copier_table_distante(
        self,
        hote_distant,
        base_distante,
        port,
        utilisateur,
        motdepasse,
        schema_initial,
        table_initiale,
        schema_final,
        table_finale,
    ):
        """
        Copie une table d'une base de données distante dans la base locale
        """
        nom_serveur_temporaire = "serveur_tmp" + str(randint(1, 100000))
        valid, nb = self.mettre_en_place_serveur_distant_fdw(
            hote_distant,
            base_distante,
            port,
            utilisateur,
            motdepasse,
            nom_serveur_temporaire,
        )
        pg_distant = PGScript(
            hote_distant, base_distante, port, utilisateur, motdepasse, log=True
        )
        champs = pg_distant.lister_champs(schema_initial, table_initiale)
        valid2, nb = self.creer_table_etrangere(
            schema_final,
            table_finale + "_fdw",
            schema_initial,
            table_initiale,
            champs,
            nom_serveur_temporaire,
        )
        valid3, nb = self.copier_table(
            schema_final, table_finale + "_fdw", schema_final, table_finale
        )
        valid4, nb = self.effacer_table_etrangere(schema_final, table_finale + "_fdw")
        valid5, nb = self.effacer_user_mapping_pour_serveur_distant_fdw(
            utilisateur, nom_serveur_temporaire
        )
        valid6, nb = self.effacer_serveur_distant_fdw(nom_serveur_temporaire)
        self.conn.deconnexion_postgres()  # permettre d'effacer la connexion postgres_fdw qui sinon restera en attente inutilement
        self.conn.connexion_postgres()  # reconnexion dans la foulée.
        pg_distant.conn.deconnexion_postgres()
        return (
            (
                True,
                1,
            )
            if valid and valid2 and valid3 and valid4 and valid5 and valid6
            else (
                False,
                -1,
            )
        )

    def import_csv(self, fichier, schema, table, separateur, entete=True):
        return self.conn.copy_from_csv(
            fichier, schema, table, separateur, entete=entete
        )


class Connexion:
    """
    Classe permettant de se connecter à une base Postgresql
    """

    def __init__(
        self,
        hote=None,
        base=None,
        port=None,
        utilisateur=None,
        motdepasse=None,
        connexion_directe=True,
    ):
        """
        Constructeur qui prend les paramètres de connexion suivant:
        - hote
        - base
        - utilisateur
        - motdepasse

        Par défaut, se connecte directement à la base.
        Pour ne pas se connecter directement, mettre connexion_directe à False.
        """
        self.connexion = None
        self.conn_actif = False
        self.hote = hote
        self.base = base
        self.utilisateur = utilisateur
        self.motdepasse = motdepasse
        self.port = port
        if connexion_directe:
            self.connexion_postgres()

    def connexion_postgres(self, client_encoding="UTF-8"):
        """
        Connexion à la base PostgreSQL via les paramètres
        définis lors de la création de l'instance de la classe.

        L'encodage du client est considéré par défaut comme du UTF-8.
        Il peut etre modifier par l'argument client_encoding.
        """
        try:
            self.connexion = psycopg2.connect(
                host=self.hote,
                dbname=self.base,
                user=self.utilisateur,
                password=self.motdepasse,
                port=self.port,
            )
            self.connexion.set_client_encoding(client_encoding)
            self.conn_actif = True
        except Exception as e:
            print("Connexion impossible : ", str(e))
            self.conn_actif = False

    def deconnexion_postgres(self):
        """
        Deconnexion de la base PostgreSQL si la connexion était établie
        """
        try:
            self.connexion.close()
            self.conn_actif = False
        except Exception as e:
            print("Problème de déconnexion : ", str(e))

    def executer(self, sql):
        with self.connexion:
            with self.connexion.cursor() as curseur:
                curseur.execute(sql)
                resultat = Resultat.from_cursor(curseur)
                return resultat
                if curseur.description:
                    return curseur.description, curseur.fetchall()
                else:
                    return curseur.rowcount
        return None

    def copy_from_csv(self, fichier, schema, table, separateur, entete=True):
        with self.connexion:
            with self.connexion.cursor() as curseur:
                with open(fichier, "r", encoding="utf-8") as data:
                    if entete:
                        next(data)
                    curseur.copy_from(data, schema + "." + table, separateur, null="")
        return True


class Resultat:
    """
    Classe représentant un résultat issu d'une requête SQL
    """

    def __init__(
        self,
    ):
        self.header = None
        self.data = None
        self.rowcount = None

    @classmethod
    def from_cursor(cls, curseur):
        """
        Génère un objet Resultat depuis un "cursor" psycopg2
        """
        r = cls()
        if curseur.description:
            r.header = curseur.description
            r.data = curseur.fetchall()
        r.rowcount = curseur.rowcount
        return r

    @property
    def columns(self):
        return [c.name for c in self.header]

    @property
    def requete_select(self):
        """
        Renvoie True si des données ont été renvoyées (requetes select)
        """
        return self.data is not None

    @property
    def smart_data(self):
        """
        Renvoie un format de données en fonction du type de résultat :
            - valeur simple
            - liste de données
            - liste de tuple
            - True et "rowcount" (si la requete n'est pas une requete select)
        """
        if self.requete_select:
            if len([len(e) for e in self.data if len(e) > 1 or len(e) == 0]) == 0:
                if len(self.data) == 1:
                    return self.data[0][0]
                else:
                    return [e[0] for e in self.data]
            return self.data
        else:
            return True, self.rowcount

    @property
    def list_data(self):
        """
        Renvoie sous un format de données liste
        """
        if self.requete_select:
            if len([len(e) for e in self.data if len(e) > 1 or len(e) == 0]) == 0:
                return [e[0] for e in self.data]
            return Exception("Les données ne peuvent être renvoyer sous forme de liste")
        else:
            raise Exception("Pas de données à renvoyer sous forme de liste")

    @property
    def dataframe(self):
        """
        Renvoie sous un format Dataframe pour pandas
        """
        if self.requete_select:
            return pd.DataFrame.from_records(self.data, columns=self.columns)
        else:
            raise Exception("Pas de données à renvoyer sous forme de dataframe")

    def to_csv(self, fichier_csv):
        if self.requete_select:
            self.dataframe.to_csv(
                fichier_csv,
                sep="|",
                index=False,
                columns=self.columns,
                encoding="utf-8",
            )
        else:
            raise Exception("Pas de données csv à exporter")

    def to_xls(self, fichier_xls):
        if self.requete_select:
            self.dataframe.to_excel(
                fichier_xls, index=False, columns=self.columns, sheet_name="Resultat"
            )
        else:
            raise Exception("Pas de données xls à exporter")

    def to_html(self, fichier_html):
        if self.requete_select:
            self.dataframe.to_html(
                fichier_html,
                index=False,
                columns=self.columns,
            )
        else:
            raise Exception("Pas de données html à exporter")


if __name__ == "__main__":

    t = PGScript(
        hote="localhost",
        base="fichiersfonciers",
        utilisateur="postgres",
        motdepasse="postgres",
        port="5432",
        log=True,
    )
    t.lister_tables("public")

# eof
