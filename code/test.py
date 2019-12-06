import jaydebeapi

if __name__ == "__main__":

    c = jaydebeapi.connect(
        "com.ibm.as400.access.AS400JDBCDriver",
        "jdbc:as400://192.168.78.78;libraries=S0629D7R;",
        ["tableau", "tableau"],
        "<path to jt400-10.1.jar>", ) #TODO: Replace with absolute path to jt400-10.1.jar (e.g. D:\david\Documents\revolt.bi\packaway-poc\libs\jt400-10.1.jar


    cursor = c.cursor()

    q = "SELECT COUNT(*) FROM TABLEAU.OBJEDNAVKY"
    print("Executing {}".format(q))
    cursor.execute("SELECT COUNT(*) FROM TABLEAU.OBJEDNAVKY")

    print("Result: {}".format(cursor.fetchall()))

    c.close()
