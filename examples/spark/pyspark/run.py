import dataflow
import map_transforms

from hamilton import driver


def main():
    dr = driver.Builder().with_modules(dataflow, map_transforms).build()
    dr.visualize_execution(["final_result"], "./out.png", {"format": "png"})
    final_result = dr.execute(["final_result"])
    print(final_result)


if __name__ == "__main__":
    main()
