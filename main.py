from fabricutils import appconfig

if __name__ == "__main__":
    mappings = appconfig.read_lakehouse_mappings()

    print(mappings)
