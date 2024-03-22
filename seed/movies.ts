import { Movie, MovieCast, MovieAward, MovieCrewRole } from "../shared/types";

export const movies: Movie[] = [
  {
    movieId: 1234,
    genre_ids: [28, 14, 32],
    original_language: "English",
    overview:
      "Every six years, an ancient order of jiu-jitsu fighters joins forces to battle a vicious race of alien invaders. But when a celebrated war hero goes down in defeat, the fate of the planet and mankind hangs in the balance.",
    popularity: 2633.943,
    release_date: "2020-11-20",
    title: "Title 1234",
    video: false,
    vote_average: 5.9,
    vote_count: 111,
  },
  {
    movieId: 4567,
    genre_ids: [28, 14, 32],
    original_language: "French",
    overview:
      "Every six years, an ancient order of jiu-jitsu fighters joins forces to battle a vicious race of alien invaders. But when a celebrated war hero goes down in defeat, the fate of the planet and mankind hangs in the balance.",
    popularity: 2633.943,
    release_date: "2020-11-20",
    title: "Title 1234",
    video: false,
    vote_average: 5.9,
    vote_count: 111,
  },
  {
    movieId: 2345,
    genre_ids: [28, 14, 32],
    original_language: "English",
    overview:
      "Every six years, an ancient order of jiu-jitsu fighters joins forces to battle a vicious race of alien invaders. But when a celebrated war hero goes down in defeat, the fate of the planet and mankind hangs in the balance.",
    popularity: 2633.943,
    release_date: "2020-11-21",
    title: "Title 2345",
    video: false,
    vote_average: 5.9,
    vote_count: 111,
  },
  {
    movieId: 3456,
    genre_ids: [28, 14, 32],
    original_language: "English",
    overview:
      "Every six years, an ancient order of jiu-jitsu fighters joins forces to battle a vicious race of alien invaders. But when a celebrated war hero goes down in defeat, the fate of the planet and mankind hangs in the balance.",
    popularity: 2633.943,
    release_date: "2020-11-21",
    title: "Title 3456",
    video: false,
    vote_average: 5.9,
    vote_count: 111,
  },
];

export const movieCasts: MovieCast[] = [
  {
    movieId: 1234,
    actorName: "Joe Bloggs",
    roleName: "Male Character 1",
    roleDescription: "description of character 1",
  },
  {
    movieId: 1234,
    actorName: "Alice Broggs",
    roleName: "Female Character 1",
    roleDescription: "description of character 2",
  },
  {
    movieId: 1234,
    actorName: "Joe Cloggs",
    roleName: "Male Character 2",
    roleDescription: "description of character 3",
  },
  {
    movieId: 2345,
    actorName: "Joe Bloggs",
    roleName: "Male Character 1",
    roleDescription: "description of character 3",
  },
];

export const movieAwards: MovieAward[] = [
  {
    movieId: 1234,
    awardBody: "Oscars",
    numAwards: 2,
    awardDescription: "Best actor and Best Costume Design",
  },
  {
    movieId: 1234,
    awardBody: "Baftas",
    numAwards: 1,
    awardDescription: "Best actor in a supporting role",
  },
  {
    movieId: 2345,
    awardBody: "Oscars",
    numAwards: 1,
    awardDescription: "Best actress in a supporting role ",
  },
  {
    movieId: 2345,
    awardBody: "GoldenGlobes",
    numAwards: 2,
    awardDescription: "Best actor and Best Director",
  },
];

export const movieCrew: MovieCrewRole[] = [
  {
    movieId: 1234,
    crewRole: "director",
    names: "Joe Bloggs, P Smith",
  },
  {
    movieId: 1234,
    crewRole: "camera",
    names: "Jane Briggs, W Smith, P Tan ",
  },
  {
    movieId: 2345,
    crewRole: "producer",
    names: "L Tamoko",
  },
  {
    movieId: 2345,
    crewRole: "camera",
    names: "O Marino, W Smith, T Ramos",
  },
];