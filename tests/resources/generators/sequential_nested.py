# from hamilton.htypes import Sequential, Collect
#
#
# # input
# def number_of_steps() -> int:
#     return 5
#
#
# # input
# def number_of_substeps() -> int:
#     return 3
#
#
# # expand
# def steps(number_of_steps: int) -> Sequential[int]:
#     for i in range(number_of_steps):
#         yield i
#
#
# # expand
# def substeps(number_of_substeps: int, steps: int) -> Sequential[int]:
#     for i in range(number_of_substeps):
#         yield i
#
#
# # process
# def squared(substeps: int) -> int:
#     return substeps ** 2
#
#
# # join
# def sum_substep_squared(squared: Collect[int]) -> int:
#     out = 0
#     for step in squared:
#         out += step
#     return out
#
#
# # join
# def final_result(sum_substep_squared: Collect[int]) -> int:
#     out = 0
#     for step in sum_substep_squared:
#         out += step
#     return out
#
# def _calc():
#     number_of_steps_ = number_of_steps()
#     number_of_substeps_ = number_of_substeps()
#     steps_ = steps(number_of_steps_)
#     substeps_ = substeps(number_of_substeps_)
#     squared_ = [squared(substep) for substep in substeps_]
#     sum_substep_squared_ = sum_substep_squared(squared_)
#     return final_result(sum_substep_squared_)
