from sqlalchemy import create_engine, text

# Define the database URL
DATABASE_URL = "sqlite:///Healthcare_Management.db"  # Replace with your desired database URL

# Read the DDL script content
with open("datasets/created/Healthcare_Management_0.sql", "r") as script_file:
    ddl_script = script_file.read()

# Split the script into individual statements
ddl_statements = ddl_script.split(';')

# Create an SQLAlchemy engine
engine = create_engine(DATABASE_URL)

# Execute each DDL statement
with engine.connect() as connection:
    for statement in ddl_statements:
        if statement.strip():  # Check if the statement is not empty
            connection.execute(text(statement))

print("Script executed successfully.")