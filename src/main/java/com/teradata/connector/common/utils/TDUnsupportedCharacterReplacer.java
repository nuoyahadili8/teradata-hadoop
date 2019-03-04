package com.teradata.connector.common.utils;

import java.util.BitSet;


/**
 * @author Administrator
 */
public class TDUnsupportedCharacterReplacer {
    public static final int[][] UNSUPPORTED_CHARS = new int[][]{{26, 1}, {888, 2}, {895, 5}, {907, 1}, {909, 1}, {930, 1}, {1320, 9}, {1367, 2}, {1376, 1}, {1416, 1}, {1419, 6}, {1480, 8}, {1515, 5}, {1525, 11}, {1540, 2}, {1564, 2}, {1806, 1}, {1867, 2}, {1970, 14}, {2043, 5}, {2094, 2}, {2111, 1}, {2140, 2}, {2143, 161}, {2424, 1}, {2432, 1}, {2436, 1}, {2445, 2}, {2449, 2}, {2473, 1}, {2481, 1}, {2483, 3}, {2490, 2}, {2501, 2}, {2505, 2}, {2511, 8}, {2520, 4}, {2526, 1}, {2532, 2}, {2556, 5}, {2564, 1}, {2571, 4}, {2577, 2}, {2601, 1}, {2609, 1}, {2612, 1}, {2615, 1}, {2618, 2}, {2621, 1}, {2627, 4}, {2633, 2}, {2638, 3}, {2642, 7}, {2653, 1}, {2655, 7}, {2678, 11}, {2692, 1}, {2702, 1}, {2706, 1}, {2729, 1}, {2737, 1}, {2740, 1}, {2746, 2}, {2758, 1}, {2762, 1}, {2766, 2}, {2769, 15}, {2788, 2}, {2800, 1}, {2802, 15}, {2820, 1}, {2829, 2}, {2833, 2}, {2857, 1}, {2865, 1}, {2868, 1}, {2874, 2}, {2885, 2}, {2889, 2}, {2894, 8}, {2904, 4}, {2910, 1}, {2916, 2}, {2936, 10}, {2948, 1}, {2955, 3}, {2961, 1}, {2966, 3}, {2971, 1}, {2973, 1}, {2976, 3}, {2981, 3}, {2987, 3}, {3002, 4}, {3011, 3}, {3017, 1}, {3022, 2}, {3025, 6}, {3032, 14}, {3067, 6}, {3076, 1}, {3085, 1}, {3089, 1}, {3113, 1}, {3124, 1}, {3130, 3}, {3141, 1}, {3145, 1}, {3150, 7}, {3159, 1}, {3162, 6}, {3172, 2}, {3184, 8}, {3200, 2}, {3204, 1}, {3213, 1}, {3217, 1}, {3241, 1}, {3252, 1}, {3258, 2}, {3269, 1}, {3273, 1}, {3278, 7}, {3287, 7}, {3295, 1}, {3300, 2}, {3312, 1}, {3315, 15}, {3332, 1}, {3341, 1}, {3345, 1}, {3387, 2}, {3397, 1}, {3401, 1}, {3407, 8}, {3416, 8}, {3428, 2}, {3446, 3}, {3456, 2}, {3460, 1}, {3479, 3}, {3506, 1}, {3516, 1}, {3518, 2}, {3527, 3}, {3531, 4}, {3541, 1}, {3543, 1}, {3552, 18}, {3573, 12}, {3643, 4}, {3676, 37}, {3715, 1}, {3717, 2}, {3721, 1}, {3723, 2}, {3726, 6}, {3736, 1}, {3744, 1}, {3748, 1}, {3750, 1}, {3752, 2}, {3756, 1}, {3770, 1}, {3774, 2}, {3781, 1}, {3783, 1}, {3790, 2}, {3802, 2}, {3806, 34}, {3912, 1}, {3949, 4}, {3992, 1}, {4029, 1}, {4045, 1}, {4059, 37}, {4294, 10}, {4349, 3}, {4681, 1}, {4686, 2}, {4695, 1}, {4697, 1}, {4702, 2}, {4745, 1}, {4750, 2}, {4785, 1}, {4790, 2}, {4799, 1}, {4801, 1}, {4806, 2}, {4823, 1}, {4881, 1}, {4886, 2}, {4955, 2}, {4989, 3}, {5018, 6}, {5109, 11}, {5789, 3}, {5873, 15}, {5901, 1}, {5909, 11}, {5943, 9}, {5972, 12}, {5997, 1}, {6001, 1}, {6004, 12}, {6110, 2}, {6122, 6}, {6138, 6}, {6159, 1}, {6170, 6}, {6264, 8}, {6315, 5}, {6390, 10}, {6429, 3}, {6444, 4}, {6460, 4}, {6465, 3}, {6510, 2}, {6517, 11}, {6572, 4}, {6602, 6}, {6619, 3}, {6684, 2}, {6751, 1}, {6781, 2}, {6794, 6}, {6810, 6}, {6830, 82}, {6988, 4}, {7037, 3}, {7083, 3}, {7098, 6}, {7156, 8}, {7224, 3}, {7242, 3}, {7296, 80}, {7411, 13}, {7655, 21}, {7958, 2}, {7966, 2}, {8006, 2}, {8014, 2}, {8024, 1}, {8026, 1}, {8028, 1}, {8030, 1}, {8062, 2}, {8117, 1}, {8133, 1}, {8148, 2}, {8156, 1}, {8176, 2}, {8181, 1}, {8191, 1}, {8293, 5}, {8306, 2}, {8335, 1}, {8349, 3}, {8378, 22}, {8433, 15}, {8586, 6}, {9204, 12}, {9255, 25}, {9291, 21}, {9984, 1}, {10187, 1}, {10189, 1}, {11085, 3}, {11098, 166}, {11311, 1}, {11359, 1}, {11506, 7}, {11558, 10}, {11622, 9}, {11633, 14}, {11671, 9}, {11687, 1}, {11695, 1}, {11703, 1}, {11711, 1}, {11719, 1}, {11727, 1}, {11735, 1}, {11743, 1}, {11826, 78}, {11930, 1}, {12020, 12}, {12246, 26}, {12284, 4}, {12352, 1}, {12439, 2}, {12544, 5}, {12590, 3}, {12687, 1}, {12731, 5}, {12772, 12}, {12831, 1}, {13055, 1}, {19894, 10}, {40908, 52}, {42125, 3}, {42183, 9}, {42540, 20}, {42612, 8}, {42648, 8}, {42744, 8}, {42895, 1}, {42898, 14}, {42922, 80}, {43052, 4}, {43066, 6}, {43128, 8}, {43205, 9}, {43226, 6}, {43260, 4}, {43348, 11}, {43389, 3}, {43470, 1}, {43482, 4}, {43488, 32}, {43575, 9}, {43598, 2}, {43610, 2}, {43644, 4}, {43715, 24}, {43744, 33}, {43783, 2}, {43791, 2}, {43799, 9}, {43815, 1}, {43823, 145}, {44014, 2}, {44026, 6}, {55204, 12}, {55239, 4}, {55292, 2052}, {64046, 2}, {64110, 2}, {64218, 38}, {64263, 12}, {64280, 5}, {64311, 1}, {64317, 1}, {64319, 1}, {64322, 1}, {64325, 1}, {64450, 17}, {64832, 16}, {64912, 2}, {64968, 40}, {65022, 2}, {65050, 6}, {65063, 9}, {65107, 1}, {65127, 1}, {65132, 4}, {65141, 1}, {65277, 2}, {65280, 1}, {65471, 3}, {65480, 2}, {65488, 2}, {65496, 2}, {65501, 3}, {65511, 1}, {65519, 10}, {65533, 3}};
    public static final BitSet UNSUPPORTED_UNICODE = new BitSet(65536);

    public static String evaluate(final String s, final char replacementChar) {
        if (s == null) {
            return null;
        }
        final char[] charArray = s.toCharArray();
        for (int i = 0; i < charArray.length; ++i) {
            if (TDUnsupportedCharacterReplacer.UNSUPPORTED_UNICODE.get(charArray[i])) {
                charArray[i] = replacementChar;
            }
        }
        return String.valueOf(charArray);
    }

    static {
        for (int i = 0; i < UNSUPPORTED_CHARS.length; ++i) {
            for (int j = 0; j < UNSUPPORTED_CHARS[i][1]; ++j) {
                UNSUPPORTED_UNICODE.set(UNSUPPORTED_CHARS[i][0] + j);
            }
        }
    }
}