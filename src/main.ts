// /**
//  * Inside this file you will use the classes and functions from rx.js
//  * to add visuals to the svg element in index.html, animate them, and make them interactive.
//  *
//  * Study and complete the tasks in observable exercises first to get ideas.
//  *
//  * Course Notes showing Asteroids in FRP: https://tgdwyer.github.io/asteroids/
//  *
//  * You will be marked on your functional programming style
//  * as well as the functionality that you implement.
//  *
//  * Document your code!
//  */
import "./style.css";
import {
    from,
    fromEvent,
    interval,
    merge,
    timer,
    mergeMap,
    Observable,
    combineLatest,
} from "rxjs";
import { map, filter, scan } from "rxjs/operators";
import * as Tone from "tone";
import { SampleLibrary } from "./tonejs-instruments";

// /** Constants */

const Viewport = {
    CANVAS_WIDTH: 200,
    CANVAS_HEIGHT: 400,
} as const;

const Constants = {
    TICK_RATE_MS: 20,
    // SONG_NAME: "amongUs",
    // SONG_NAME: "armageddon",
    // SONG_NAME: "BeethovenVirus",
    // SONG_NAME: "butterflylovers",
    // SONG_NAME: "DifficultMode",
    // SONG_NAME: "ditto",
    // SONG_NAME: "Drama",
    // SONG_NAME: "HungarianDanceNo5",
    // SONG_NAME: "HungarianFolkSongs",
    // SONG_NAME: "IWonder",
    // SONG_NAME: "Liebestraum",
    // SONG_NAME: "lifeisstillgoingon",
    // SONG_NAME: "luciddream",
    // SONG_NAME: "maestro",
    // SONG_NAME: "Melody",
    // SONG_NAME: "playingwithfire",
    // SONG_NAME: "RightMyWrongs",
    // SONG_NAME: "RockinRobin",
    // SONG_NAME: "Runaway",
    // SONG_NAME: "SleepingBeauty",
    // SONG_NAME: "SwamLake",
    // SONG_NAME: "ThroughTheFireAndTheFlames_easy",
    SONG_NAME: "ThroughTheFireAndTheFlames_hard",
    // SONG_NAME: "TokyoGhoulOP2",
    // SONG_NAME: "TokyoGhoulOP3",
    // SONG_NAME: "TurkishMarch",
    // SONG_NAME: "UndertaleNoMoreDeals",
} as const;

const Note = {
    RADIUS: 0.07 * Viewport.CANVAS_WIDTH,
    TAIL_WIDTH: 10,
};

// /** User input */

type Key = "KeyH" | "KeyJ" | "KeyK" | "KeyL";

type Event = "keydown" | "keyup" | "keypress";

// /** State processing */

// `State` represents the current state of the game
type State = Readonly<{
    gameEnd: boolean;
    notes: ReadonlyArray<NoteObj>;
    multiplier: number;
    score: number;
    highScore: number;
    circles: ReadonlyArray<Circle>;
    exits: ReadonlyArray<Circle>;
    strikeCount: number;
}>;

// Initial game state
const initialState: State = {
    gameEnd: false,
    notes: [],
    multiplier: 0,
    score: 0,
    highScore: 0,
    circles: [],
    exits: [],
    strikeCount: 0,
} as const;

/**
 * NoteObj represents the note with its properties
 */
type NoteObj = Readonly<{
    userPlayed: boolean;
    instrumentName: string;
    velocity: number;
    pitch: number;
    start: number;
    end: number;
}>;

/**
 * Circle represents a visual circle on the SVG canvas tied to a note
 */
type Circle = Readonly<{
    note: NoteObj;
    circle: SVGElement;
    yPos: number;
}>;

/**
 * Action interface for state transformations
 */
interface Action {
    apply(s: State): State;
}

/**
 * code adopted from Week 4 applied
 */
abstract class RNG {
    // LCG using GCC's constants
    private static m = 0x80000000; // 2**31
    private static a = 1103515245;
    private static c = 12345;

    /**
     * Call `hash` repeatedly to generate the sequence of hashes.
     * @param seed
     * @returns a hash of the seed
     */
    public static hash = (seed: number) => (RNG.a * seed + RNG.c) % RNG.m;

    /**
     * Takes hash value and scales it to the range [-1, 1]
     */
    public static scale = (hash: number) => (2 * hash) / (RNG.m - 1) - 1;
}

/**
 * Converts values in a stream to random numbers in the range [-1, 1]
 *
 * This usually would be implemented as an RxJS operator, but that is currently
 * beyond the scope of this course.
 *
 * @param source$ The source Observable, elements of this are replaced with random numbers
 * @param seed The seed for the random number generator
 */
export function createRngStreamFromSource<T>(source$: Observable<T>) {
    return function createRngStream(seed: number = 0): Observable<number> {
        const randomNumberStream = source$.pipe(
            scan((acc) => RNG.hash(acc), seed),
            map((hash) => RNG.scale(hash)),
        );

        return randomNumberStream;
    };
}

/**
 * Tick action advances the game state by moving the circles downward.
 */
class Tick implements Action {
    apply(s: State): State {
        const updatedCircles = s.circles.map(({ note, circle, yPos }) => {
            const updatedY = yPos + 4;
            return { note: note, circle: circle, yPos: updatedY };
        });

        const [circlesToKeep, circlesToExit] = updatedCircles.reduce<
            [Circle[], Circle[]]
        >(
            ([keep, exit], circle) =>
                circle.yPos >= 350
                    ? [keep, [...exit, circle]]
                    : [[...keep, circle], exit],
            [[], []],
        );

        return {
            ...s,
            circles: circlesToKeep,
            exits: [...s.exits, ...circlesToExit],
        };
    }
}

// /** Utility functions */

const show = (elem: SVGGraphicsElement) => {
    elem.setAttribute("visibility", "visible");
    elem.parentNode?.appendChild(elem);
};

const hide = (elem: SVGGraphicsElement) =>
    elem.setAttribute("visibility", "hidden");

const createSvgElement = (
    namespace: string | null,
    name: string,
    props: Record<string, string> = {},
) => {
    const elem = document.createElementNS(namespace, name) as SVGElement;
    Object.entries(props).forEach(([k, v]) => elem.setAttribute(k, v));
    return elem;
};

/**
 * Calculates the score multiplier based on the number of successful strikes.
 * @param strikeCount The number of successful strikes.
 * @returns The calculated multiplier.
 */
const calculateMultiplier = (strikeCount: number): number => {
    return 1 + Math.floor(strikeCount / 10) * 0.2;
};

/**
 * Highlights the control element associated with the pressed key.
 * @param key The key code of the pressed key.
 */
function highlightControl(key: Key) {
    // Define the highlight classes based on the pressed key
    const highlightClasses: { [key in Key]: string } = {
        KeyH: "highlight-green",
        KeyJ: "highlight-red",
        KeyK: "highlight-blue",
        KeyL: "highlight-yellow",
    };

    // Get all control elements
    const controls = document.querySelectorAll(".control_label");

    // Remove all highlight classes
    controls.forEach((control) => {
        control.classList.remove(
            "highlight-green",
            "highlight-red",
            "highlight-blue",
            "highlight-yellow",
        );
    });

    // Add the highlight class to the control corresponding to the pressed key
    const highlightClass = highlightClasses[key];
    if (highlightClass) {
        const control = document.getElementById(`${key}Control`);
        if (control) {
            control.classList.add(highlightClass);
        }
    }
}

function removeHighlight(key: string) {
    // Define the highlight classes based on the released key
    const highlightClasses: { [key: string]: string } = {
        KeyH: "highlight-green",
        KeyJ: "highlight-red",
        KeyK: "highlight-blue",
        KeyL: "highlight-yellow",
    };

    // Remove the highlight class from the control corresponding to the released key
    const highlightClass = highlightClasses[key];
    if (highlightClass) {
        const control = document.getElementById(`${key}Control`);
        if (control) {
            control.classList.remove(highlightClass);
        }
    }
}

/**
 * Parses a CSV string representing musical notes into an array of NoteObj.
 * @param csvContents The CSV string containing note data.
 * @returns An array of NoteObj representing the notes.
 */
function parseCsv(csvContents: string) {
    const rows = csvContents.split("\n");

    const notes = rows.slice(1).map((row) => {
        const [userPlayed, instrumentName, velocity, pitch, start, end] =
            row.split(",");

        return {
            userPlayed: userPlayed === "True",
            instrumentName: instrumentName,
            velocity: parseInt(velocity) / 127,
            pitch: parseInt(pitch),
            start: parseFloat(start),
            end: parseFloat(end),
        };
    });
    return notes;
}

/**
 * Returns an observable that emits a random instrument name from a provided array of instruments.
 * The selection is based on values from the provided random number stream.
 * @param array The array of instrument names.
 * @param rng$ An observable emitting random numbers in the range [-1, 1].
 * @returns An observable emitting a random instrument name.
 */
function getRandomInstrument<T>(
    array: T[],
    rng$: Observable<number>,
): Observable<T> {
    return rng$.pipe(
        map((rng) => {
            const index = Math.floor((rng + 1) * 0.5 * array.length);
            return array[index];
        }),
    );
}

/**
 * Returns an observable that emits a random pitch value between 21 (A0) and 108 (C8).
 * @param rng$ An observable emitting random numbers in the range [-1, 1].
 * @returns An observable emitting a random pitch value.
 */
function getRandomPitch(rng$: Observable<number>): Observable<number> {
    return rng$.pipe(
        map((rng) => {
            const minPitch = 21; // A0 in MIDI
            const maxPitch = 108; // C8 in MIDI
            return Math.floor(
                (rng + 1) * 0.5 * (maxPitch - minPitch + 1) + minPitch,
            );
        }),
    );
}

/**
 * Returns an observable that emits a random note duration between 0 and 0.5 seconds.
 * @param rng$ An observable emitting random numbers in the range [-1, 1].
 * @returns An observable emitting a random duration value.
 */
function getRandomDuration(rng$: Observable<number>): Observable<number> {
    return rng$.pipe(
        map((rng) => (rng + 1) * 0.25), // Random duration between 0 and 0.5 seconds
    );
}

/**
 * Returns an observable that emits a random velocity value between 0 and 1.
 * @param rng$ An observable emitting random numbers in the range [-1, 1].
 * @returns An observable emitting a random velocity value.
 */
function getRandomVelocity(rng$: Observable<number>): Observable<number> {
    return rng$.pipe(map((rng) => (rng + 1) * 0.5));
}

export function main(
    csvContents: string,
    samples: { [key: string]: Tone.Sampler },
) {
    const notes: NoteObj[] = parseCsv(csvContents);
    const svg = document.querySelector("#svgCanvas") as SVGGraphicsElement &
        HTMLElement;
    const gameover = document.querySelector("#gameOver") as SVGGraphicsElement &
        HTMLElement;

    svg.setAttribute("height", `${Viewport.CANVAS_HEIGHT}`);
    svg.setAttribute("width", `${Viewport.CANVAS_WIDTH}`);

    const key$ = fromEvent<KeyboardEvent>(document, "keypress").pipe(
        filter(({ repeat }) => !repeat),
    );

    const fromKey = (keyCode: Key) =>
        key$.pipe(filter(({ code }) => code === keyCode));

    const keyH$ = fromKey("KeyH");
    const keyJ$ = fromKey("KeyJ");
    const keyK$ = fromKey("KeyK");
    const keyL$ = fromKey("KeyL");

    const keyPress$ = merge(keyH$, keyJ$, keyK$, keyL$).pipe(
        map((event) => {
            const key = event.code as Key;
            highlightControl(key);
            return { key };
        }),
    );

    const keyUp$ = fromEvent<KeyboardEvent>(document, "keyup").pipe(
        map((event) => {
            removeHighlight(event.code);
            return { key: event.code as Key };
        }),
    );
    /**
     * Checks if the provided key is a valid game key.
     * @param key The key to check.
     * @returns true if the key is valid, otherwise false.
     */
    const isValidKey = (key: string): key is Key => {
        return ["KeyH", "KeyJ", "KeyK", "KeyL"].includes(key);
    };

    /**
     * Maps a key press to its corresponding column ID
     * @param key The key to map.
     * @returns The column ID corresponding to the key.
     */
    const mapKeyToColumnId = (key: Key): number => {
        const keyColumnMap: Record<Key, number> = {
            KeyH: 20,
            KeyJ: 40,
            KeyK: 60,
            KeyL: 80,
        };
        return keyColumnMap[key];
    };

    const tick$ = interval(Constants.TICK_RATE_MS);

    /**
     * Triggers the playback of a note using the provided Tone.js Sampler.
     * @param note The note object to be play.
     * @param samples samples from tonejs-instrument.
     */
    const playNote = (
        note: NoteObj,
        samples: { [key: string]: Tone.Sampler },
    ) => {
        samples[note.instrumentName]?.triggerAttackRelease(
            Tone.Frequency(note.pitch, "midi").toNote(),
            note.end - note.start,
            undefined,
            note.velocity,
        );
    };

    /**
     * Triggers the playback of a randomly generated note using the provided Tone.js Sampler.
     * @param instrument The name of the instrument to use.
     * @param pitch The pitch of the note to play.
     * @param duration The duration of the note.
     * @param velocity The velocity  of the note.
     * @param samples samples from tonejs-instrument.
     */
    const playRandomNote = (
        instrument: string,
        pitch: number,
        duration: number,
        velocity: number,
        samples: { [key: string]: Tone.Sampler },
    ) => {
        samples[instrument]?.triggerAttackRelease(
            Tone.Frequency(pitch, "midi").toNote(),
            duration,
            undefined,
            velocity,
        );
    };

    function createNoteCircle(note: NoteObj) {
        const columnId = note.pitch % 4;
        console.log(`${columnId}`);
        const colours = ["green", "red", "blue", "yellow"];

        const circle = createSvgElement(svg.namespaceURI, "circle", {
            r: `${Note.RADIUS}`,
            cx: `${(columnId + 1) * 20}%`,
            cy: "0",
            style: `fill: ${colours[columnId]};`,
            class: "shadow",
        });

        return {
            note: note,
            circle: circle,
            yPos: 0,
        };
    }

    /**
     * Creates an SVG circle element representing a note and assigns it a color based on the column.
     * @param note The note object containing the properties of the note.
     * @returns A Circle object containing the note and its corresponding SVG circle element
     */
    const addCircleToState = (s: State, circle: Circle) => {
        return {
            ...s,
            circles: [...s.circles, circle],
        };
    };

    // An observable to handle all the note that user do not need to play, act as a background music
    const nonUserPlayedNotes$ = from(notes).pipe(
        filter((note) => !note.userPlayed),
        mergeMap((note) =>
            timer(note.start * 1000).pipe(
                map(() => (state: State) => {
                    playNote(note, samples);
                    return state;
                }),
            ),
        ),
    );

    // An observable to handle all the note that user need to play
    const userPlayNote$ = from(notes).pipe(
        filter((note) => note.userPlayed),
        mergeMap((note) =>
            timer(note.start * 1000 - 1700).pipe(
                map(() => (state: State) => {
                    const noteCircle = createNoteCircle(note);
                    return addCircleToState(state, noteCircle);
                }),
            ),
        ),
    );

    // An observable to handle the key that user pressed
    const handleKeyPress$ = merge(keyPress$, keyUp$).pipe(
        map(({ key }) => (s: State) => {
            if (!isValidKey(key)) return s;

            const columnId = mapKeyToColumnId(key);

            const targetCircle = s.circles.find((circle) => {
                const cx = circle.circle.getAttribute("cx");
                if (cx !== null) {
                    return parseFloat(cx) === columnId;
                }
            });

            // Return if no matching circle
            if (!targetCircle) return s;

            // Play the note when it correctly aligned with the note
            if (targetCircle.yPos >= 315) {
                playNote(targetCircle.note, samples);

                // Remove the played circle
                const updatedCircles = s.circles.filter(
                    (circle) => circle !== targetCircle,
                );

                return {
                    ...s,
                    circles: updatedCircles,
                    exits: [...s.exits, targetCircle],
                    score: s.score + 3 * s.multiplier,
                    strikeCount: s.strikeCount + 1,
                    multiplier: calculateMultiplier(s.strikeCount + 1),
                };
            } else {
                // If the note was not pressed correctly, play a random note
                const rng$ = createRngStreamFromSource(timer(0))(
                    s.score + 2000,
                );
                const randomInstrument$ = getRandomInstrument(
                    Object.keys(samples),
                    rng$,
                );

                const randomPitch$ = getRandomPitch(rng$);
                const randomDuration$ = getRandomDuration(rng$);
                const randomVelocity$ = getRandomVelocity(rng$);

                // Combine the randomly generated instrument, pitch, duration, and velocity to play a random note
                playNote(targetCircle.note, samples);
                // combineLatest([
                //     randomInstrument$,
                //     randomPitch$,
                //     randomDuration$,
                //     randomVelocity$,
                // ]).subscribe(([instrument, pitch, duration, velocity]) => {
                //     playRandomNote(
                //         instrument,
                //         pitch,
                //         duration,
                //         velocity,
                //         samples,
                //     );
                // });

                return {
                    ...s,
                    score: s.score - 2,
                    strikeCount: 0,
                    multiplier: 1,
                };
            }
        }),
    );

    const render = (s: State) => {
        // Update the position of each circle on the screen based on its yPos value
        s.circles.forEach(({ circle, yPos }) => {
            circle.setAttribute("cy", String(yPos));
            svg.appendChild(circle);
        });

        // Hide circles that should be removed
        s.exits.forEach(({ circle }) => hide(circle as SVGGraphicsElement));

        const scoreText = document.getElementById("scoreText");
        const multiplierText = document.getElementById("multiplierText");

        if (scoreText) {
            scoreText.textContent = s.score.toFixed(1);
        }

        if (multiplierText) {
            multiplierText.textContent = s.multiplier.toFixed(1);
        }
    };

    // The main stream that combines different observables to manage the game's state
    const source$ = merge(
        tick$.pipe(map(() => (state: State) => new Tick().apply(state))),
        userPlayNote$,
        nonUserPlayedNotes$,
        handleKeyPress$,
    )
        .pipe(
            scan(
                (state: State, reduceState) => reduceState(state),
                initialState,
            ),
        )
        .subscribe((state: State) => {
            render(state);
            if (state.gameEnd) {
                show(gameover);
            } else {
                hide(gameover);
            }
        });
}

if (typeof window !== "undefined") {
    const samples = SampleLibrary.load({
        instruments: [
            "bass-electric",
            "violin",
            "piano",
            "trumpet",
            "saxophone",
            "trombone",
            "flute",
        ],
        baseUrl: "samples/",
    });

    const startGame = (contents: string) => {
        document.body.addEventListener(
            "mousedown",
            () => main(contents, samples),
            { once: true },
        );
    };

    const { protocol, hostname, port } = new URL(import.meta.url);
    const baseUrl = `${protocol}//${hostname}${port ? `:${port}` : ""}`;

    Tone.ToneAudioBuffer.loaded().then(() => {
        for (const instrument in samples) {
            samples[instrument].toDestination();
            samples[instrument].release = 0.5;
        }

        fetch(`${baseUrl}/assets/${Constants.SONG_NAME}.csv`)
            .then((response) => response.text())
            .then((text) => startGame(text))
            .catch((error) =>
                console.error("Error fetching the CSV file:", error),
            );
    });
}
