import fitz  # PyMuPDF
import os
import pandas as pd


def find_top_left_of_table(page, search_text):
    text_instances = page.search_for(search_text)
    if text_instances:
        for inst in text_instances:
            x0, y0, x1, y1 = inst
            inst = fitz.Rect(0, y1, page.rect.width, y1)
            return inst
    raise ValueError(f"Text '{search_text}' not found in whole page")


def find_bottom_right_of_table(page):
    lines = page.get_drawings()
    drawing_types = set(line['type'] for line in lines)
    print(f"Drawing types found: {drawing_types}")
    for line in lines:
        if line['type'] == 'line':
            x0, y0, x1, y1 = line['rect']
            if y0 == y1 and line['color'] == (0, 0, 0):  # Horizontal black line
                return (x0, y0, x1, y1)
    raise ValueError("Black line spanning the whole page not found")

def extract_tables_from_pdf(pdf_path):
    tables_data = []
    document = fitz.open(pdf_path)
    for page_num in range(len(document)):
        page = document.load_page(page_num)
        tabs = page.find_tables()
        if len(tabs.tables) == 0:
            # AllStar format
            x0, y0, x1, y1 = find_top_left_of_table(page, "For the Month Ending")
            x2, y2, x3, y3 = find_bottom_right_of_table(page)
            rect = fitz.Rect(x0, y0, x3, y3)  # Adjust the height as needed
            tabs = page.find_tables(rect)
        if len(tabs.tables) == 0:
            tabs = page.find_tables(horizontal_strategy='text', vertical_strategy='text')
        if len(tabs.tables) == 0:
            raise ValueError(f"No tables found in page {page_num} of {pdf_path}")
        for table in tabs.tables:
            tables_data.append(table.extract())
            # for row in table.rows:
            #     # row = [span['text'] for span in line['spans']]
            #     tables_data.append()
    return tables_data

def save_tables_to_parquet(tables_data, pdf_path):
    output_dir = os.path.dirname(pdf_path)
    base_filename = os.path.splitext(os.path.basename(pdf_path))[0]
    output_filename = os.path.join(output_dir, f"{base_filename}.parquet")
    df = pd.DataFrame(tables_data)
    df.to_parquet(output_filename, engine='pyarrow')

def process_pdfs(input_dir):
    for root, _, files in os.walk(input_dir):
        for filename in files:
            if filename.endswith('.pdf'):
                pdf_path = os.path.join(root, filename)
                tables_data = extract_tables_from_pdf(pdf_path)
                save_tables_to_parquet(tables_data, pdf_path)

input_directory = './sample/2024/AllStar/1. January/TY1'  # './sample'
process_pdfs(input_directory)