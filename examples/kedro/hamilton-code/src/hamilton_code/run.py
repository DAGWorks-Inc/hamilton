import data_processing
import data_science

from hamilton import driver


def main():
    dr = driver.Builder().with_modules(data_processing, data_science).build()
    dr.display_all_functions("all_functions.png")

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
    results = dr.execute(["evaluate_model"], inputs=inputs)
    print(results)


if __name__ == "__main__":
    main()
