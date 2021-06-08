package org.reco.sys.content.similarity;

import java.util.List;

public class Movie {

	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;

	private int movieId;
	
	private String title;
	
	private List<String> genres;
	
	public static final String EXCLUDE = "no genres listed";
	
	/*
	 * private static final String[] = {Action Adventure Animation Children Comedy
	 * Crime Documentary Drama Fantasy Film-Noir Horror IMAX Musical Mystery Romance
	 * Sci-Fi Thriller War Western}
	 */ 

	public Movie(int movieId, String title) {
		super();
		this.movieId = movieId;
		this.title = title;
	}

	public int getMovieId() {
		return movieId;
	}

	public void setMovieId(int movieId) {
		this.movieId = movieId;
	}

	public String getTitle() {
		return title;
	}

	public void setTitle(String title) {
		this.title = title;
	}

	public List<String> getGenres() {
		return genres;
	}

	public void setGenres(List<String> tags) {
		this.genres = tags;
	}
	
	public String toString() {
		StringBuilder builder = new StringBuilder();
		builder.append("{ MovieId: " + this.movieId).append(", Title: " + this.title ).append(", Genres: " + this.genres + " }");
		return builder.toString();
	}


}
