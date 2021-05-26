from numpy import sqrt, deg2rad

FRACTIONAL_PRECISION = 2
IMAGING_TUBE_D = 25.4
TUBE_DEPTH = 4

# Straw information.
NUM_STRAWS_PER_TUBE = 7
STRAW_DIAMETER = 8.00
STRAW_RESOLUTION = 512
STRAW_Y_LOC = 1.14
STRAW_Z_LOC = 7.67
STRAW_ALIGNMENT_OFFSET_ANGLE = deg2rad(5)
TUBE_OUTER_STRAW_DIST_FROM_CP = sqrt(STRAW_Y_LOC**2 + STRAW_Z_LOC**2)

loki_banks = {0: {'A': [(-500, -781, 5012.5),
                        (-500, -799.84, 5091.28),
                        (-500, 781, 5012.5),
                        (-500, 762.16, 5091.28)],
                  'B': [(500, -781, 5012.5),
                        (500, -799.84, 5091.28),
                        (500, 781, 5012.5),
                        (500, 762.16, 5091.28)],
                  'num_tubes': 224
                  },
              1: {'A': [(-500, -710.24, 2899.18),
                        (-500, -699.24, 2979.43),
                        (-500, -286.34, 2941.42),
                        (-500, -275.35, 3021.67)],
                  'B': [(500, -710.24, 2899.18),
                        (500, -699.24, 2979.43),
                        (500, -286.34, 2941.42),
                        (500, -275.35, 3021.67)],
                  'num_tubes': 64},
              2: {'A': [(-535.94, -250, 3328.75),
                        (-523.26, -250, 3408.75),
                        (-224.49, -250, 3353.18),
                        (-211.82, -250, 3433.19)],
                  'B': [(-535.94, 250, 3328.75),
                        (-523.26, 250, 3408.75),
                        (-224.49, 250, 3353.18),
                        (-211.82, 250, 3433.19)],
                  'num_tubes': 48},
              3: {'A': [(-500, 286.33, 2941.34),
                        (-500, 275.34, 3021.59),
                        (-500, 710.23, 2899.11),
                        (-500, 699.24, 2979.36)],
                  'B': [(500, 286.33, 2941.34),
                        (500, 275.34, 3021.59),
                        (500, 710.23, 2899.11),
                        (500, 699.24, 2979.36)],
                  'num_tubes': 64},
              4: {'A': [(224.49, -250, 3353.11),
                        (211.82, -250, 3433.11),
                        (535.93, -250, 3328.67),
                        (523.26, -250, 3408.67)],
                  'B': [(224.49, 250, 3353.11),
                        (211.82, 250, 3433.11),
                        (535.93, 250, 3328.67),
                        (523.26, 250, 3408.67)],
                  'num_tubes': 48},
              5: {'A': [(-700, -1096.67, 1051.39),
                        (-700, -1102.32, 1132.19),
                        (-700, -365.34, 1281.9),
                        (-700, -370.99, 1362.7)],
                  'B': [(500, -1096.67, 1051.39),
                        (500, -1102.32, 1132.19),
                        (500, -365.34, 1281.9),
                        (500, -370.99, 1362.7)],
                  'num_tubes': 112},
              6: {'A': [(-1191.15, -585, 1509.59),
                        (-1187.05, -585, 1590.49),
                        (-325.76, -585, 1671.47),
                        (-321.66, -585, 1752.37)],
                  'B': [(-1191.15, 615, 1509.59),
                        (-1187.05, 615, 1590.49),
                        (-325.76, 615, 1671.47),
                        (-321.66, 615, 1752.37)],
                  'num_tubes': 128},
              7: {'A': [(-500, 365.36, 1281.97),
                        (-500, 371.01, 1362.77),
                        (-500, 880, 1119.78),
                        (-500, 885.65, 1200.58)],
                  'B': [(700, 365.36, 1281.97),
                        (700, 371.01, 1362.77),
                        (700, 880, 1119.78),
                        (700, 885.65, 1200.58)],
                  'num_tubes': 80},
              8: {'A': [(325.62, -650, 1670.71),
                        (321.52, -650, 1751.61),
                        (1191.14, -650, 1509.52),
                        (1187.04, -650, 1590.41)],
                  'B': [(325.62, 550, 1670.71),
                        (321.52, 550, 1751.61),
                        (1191.14, 550, 1509.52),
                        (1187.04, 550, 1590.41)],
                  'num_tubes': 128},
              }
