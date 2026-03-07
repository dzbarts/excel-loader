try:
    loader.process_excel_data(...)
except DataValidationError as e:
    # отправить алерт, залогировать, продолжить
except FileReadError as e:
    # retry или пометить таск как skipped