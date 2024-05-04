import data_processing
import data_science

from hamilton.driver import Builder


def main():
    driver = Builder().with_modules(data_processing, data_science).build()
    driver.display_all_functions("all_functions.png")

    inputs = dict(
        data_dir="../data/",
        test_size=0.2,
        random_state=3,
        features=[
            "engines",
            "passenger_capacity",
            "crew",
            "d_check_complete",
            "moon_clearance_complete",
            "iata_approved",
            "company_rating",
            "review_scores_rating",
        ],
    )
    results = driver.execute(["evaluate_model"], inputs=inputs)
    print(results)


if __name__ == "__main__":
    main()
