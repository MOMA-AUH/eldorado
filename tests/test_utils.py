def create_files(files):
    for file in files:
        file.parent.mkdir(parents=True, exist_ok=True)
        file.touch()
