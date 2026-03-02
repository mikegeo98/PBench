import sys, re, os
from pathlib import Path

# Tables in load order (smaller tables first for faster feedback)
TABLES = [
    "comp_cast_type", "company_type", "info_type", "kind_type", "link_type", "role_type",
    "aka_name", "aka_title", "char_name", "company_name", "keyword", "name", "title",
    "cast_info", "complete_cast", "movie_companies", "movie_info", "movie_info_idx",
    "movie_keyword", "movie_link", "person_info"
]

def main():
    script_dir = Path(__file__).parent
    data_dir = Path(sys.argv[1]) if len(sys.argv) > 2 else script_dir / "imdb-data"
    out_dir = script_dir / "imdb-clean"

    os.mkdir(out_dir)
    for table in TABLES:
        with open(out_dir/f"{table}.csv", "w", encoding="utf-8", errors="replace") as nf:
            with open(data_dir/f"{table}.csv", "r", encoding="utf-8", errors="replace") as f:
                for line in f:
                    line = re.sub(r"\\\\(?=\")", "", line)
                    line = re.sub(r"\\\"", "\"\"", line)
                    nf.write(line)


if __name__ == "__main__":
    main()