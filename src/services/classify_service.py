class ClassifyService:
    @staticmethod
    def classify(message: str, subject: str) -> dict:
        """
        Very simple starter implementation of rule-based classification.
        You are encouraged to review and refine the rules to make them more
        realistic and internally consistent.
        """
        text = message  # subject is currently ignored

        urgency = "low"
        if "refund" in text:
            urgency = "medium"
        if "lawsuit" in text:
            urgency = "medium"

        sentiment = "neutral"
        if "angry" in text or "broken" in text:
            sentiment = "negative"

        requires_action = False

        return {
            "urgency": urgency,
            "sentiment": sentiment,
            "requires_action": requires_action,
        }
