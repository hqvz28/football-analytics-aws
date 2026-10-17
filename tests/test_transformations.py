"""
Unit tests for data transformation logic and derived football KPIs.
"""

import unittest


class TestFootballTransformations(unittest.TestCase):

    def calculate_kpis(self, home_score: int, away_score: int, home_shots: int, away_shots: int):
        """Pure Python replica of PySpark KPI calculation expressions."""
        total_goals = home_score + away_score
        goal_diff = home_score - away_score

        if home_score > away_score:
            outcome = "HOME_WIN"
            home_pts = 3
            away_pts = 0
        elif home_score < away_score:
            outcome = "AWAY_WIN"
            home_pts = 0
            away_pts = 3
        else:
            outcome = "DRAW"
            home_pts = 1
            away_pts = 1

        is_over_2_5 = 1 if total_goals > 2.5 else 0
        btts = 1 if (home_score > 0 and away_score > 0) else 0

        home_conv_rate = round(home_score / max(home_shots, 1), 4)
        away_conv_rate = round(away_score / max(away_shots, 1), 4)

        return {
            "total_goals": total_goals,
            "goal_diff": goal_diff,
            "outcome": outcome,
            "home_pts": home_pts,
            "away_pts": away_pts,
            "is_over_2_5": is_over_2_5,
            "btts": btts,
            "home_conv_rate": home_conv_rate,
            "away_conv_rate": away_conv_rate
        }

    def test_home_win_calculation(self):
        result = self.calculate_kpis(home_score=3, away_score=1, home_shots=15, away_shots=8)

        self.assertEqual(result["total_goals"], 4)
        self.assertEqual(result["goal_diff"], 2)
        self.assertEqual(result["outcome"], "HOME_WIN")
        self.assertEqual(result["home_pts"], 3)
        self.assertEqual(result["away_pts"], 0)
        self.assertEqual(result["is_over_2_5"], 1)
        self.assertEqual(result["btts"], 1)
        self.assertEqual(result["home_conv_rate"], 0.2)

    def test_draw_calculation(self):
        result = self.calculate_kpis(home_score=0, away_score=0, home_shots=10, away_shots=5)

        self.assertEqual(result["total_goals"], 0)
        self.assertEqual(result["goal_diff"], 0)
        self.assertEqual(result["outcome"], "DRAW")
        self.assertEqual(result["home_pts"], 1)
        self.assertEqual(result["away_pts"], 1)
        self.assertEqual(result["is_over_2_5"], 0)
        self.assertEqual(result["btts"], 0)


if __name__ == "__main__":
    unittest.main()
