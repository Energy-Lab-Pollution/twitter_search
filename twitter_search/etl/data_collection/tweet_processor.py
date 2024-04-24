
class TweetProcessor:

    @staticmethod
    def get_coordinates(client, location):
        """
        Get the latitude and longitude coordinates of a location.
        """
        if location is None:
            return (None, None)
        try:
            # Geocode the location using Google Maps Geocoding API
            geocode_result = client.geocode(location)

            # Check if any results were returned
            if geocode_result:
                lat = geocode_result[0]["geometry"]["location"]["lat"]
                lng = geocode_result[0]["geometry"]["location"]["lng"]
                return (lat, lng)
            else:
                return (None, None)
            
        except Exception as e:
            print(f"Error geocoding location '{location}': {e}")
            return (None, None)

    def process_tweets_for_users(self):
        """
        Adds tweets to each user's dictionary.

        Args:
            data: Response data containing tweets and users.

        Returns:
            None
        """
        for tweet in self.search_tweets_tweets:
            author_id = tweet.get("author_id", None)
            if author_id:
                for user in self.total_users_dict:
                    if user["user_id"] == author_id:
                        user["tweets"].append(tweet["text"])

    def geo_coder(self):
        """
        Runs the geocoding process for all users.
        """
        for user in self.total_users_dict:
            user["geo_location"] = self.get_coordinates(self.gmaps_client,user["location"])