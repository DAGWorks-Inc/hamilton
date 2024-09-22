import matplotlib.pyplot as plt


def plot_helper(X, Y, Z, land_reference, levels, species, roc_auc, i):
    # Plot map of South America
    plt.subplot(1, 2, i + 1)
    plt.contour(X, Y, land_reference, levels=[-9998], colors="k", linestyles="solid")
    plt.xticks([])
    plt.yticks([])

    # plot contours of the prediction
    plt.contourf(X, Y, Z, levels=levels, cmap=plt.cm.Reds)
    plt.colorbar(format="%.2f")

    # scatter training/testing points
    plt.scatter(
        species.pts_train["dd long"],
        species.pts_train["dd lat"],
        s=2**2,
        c="black",
        marker="^",
        label="train",
    )
    plt.scatter(
        species.pts_test["dd long"],
        species.pts_test["dd lat"],
        s=2**2,
        c="black",
        marker="x",
        label="test",
    )
    plt.legend()
    plt.title(species.name)
    plt.axis("equal")

    plt.text(-35, -70, "AUC: %.3f" % roc_auc, ha="right")
    print("\n Area under the ROC curve : %f" % roc_auc)


if __name__ == "__main__":
    import grids
    import load_data
    import postprocessing_results
    import preprocessing
    import train_and_predict

    from hamilton import driver

    dr = (
        driver.Builder()
        .with_modules(grids, load_data, postprocessing_results, preprocessing, train_and_predict)
        .build()
    )
    dr.visualize_execution(
        inputs={"chosen_species": "aaa"},
        final_vars=["plot_species_distribution"],
        output_file_path="my_dag.png",
    )

    species = ("bradypus_variegatus_0", "microryzomys_minutus_0")
    fig = plt.figure(figsize=(3, 6))
    for i, name in enumerate(species):
        print("_" * 80)
        print("Modeling distribution of species '%s'" % name)
        inputs = {"chosen_species": name}
        final_vars = ["plot_species_distribution"]
        results = dr.execute(inputs=inputs, final_vars=final_vars)[final_vars[0]]
        plot_helper(i=i, **results)
    fig.savefig("output.png")
