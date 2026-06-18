import { getMethodNames, getMethodsFor } from "./utils/methods.js";
import "./utils/window.js";
import { extend, makeInstance } from "./utils/adopter.js";
import "./modules/optional/arrange.js";
import "./modules/optional/class.js";
import "./modules/optional/css.js";
import "./modules/optional/data.js";
import "./modules/optional/memory.js";
import Color from "./types/Color.js";
import Point from "./types/Point.js";
import Matrix from "./types/Matrix.js";
import "./modules/core/parser.js";
import Box from "./types/Box.js";
import List from "./types/List.js";
import "./modules/core/selector.js";
import "./modules/core/event.js";
import EventTarget from "./types/EventTarget.js";
import SVGArray from "./types/SVGArray.js";
import SVGNumber from "./types/SVGNumber.js";
import Dom from "./elements/Dom.js";
import Element from "./elements/Element.js";
import "./modules/optional/sugar.js";
import "./modules/optional/transform.js";
import Container from "./elements/Container.js";
import Defs from "./elements/Defs.js";
import Shape from "./elements/Shape.js";
import Ellipse from "./elements/Ellipse.js";
import Fragment from "./elements/Fragment.js";
import Gradient from "./elements/Gradient.js";
import Pattern from "./elements/Pattern.js";
import Image from "./elements/Image.js";
import PointArray from "./types/PointArray.js";
import Line from "./elements/Line.js";
import Marker from "./elements/Marker.js";
import "./animation/Controller.js";
import PathArray from "./types/PathArray.js";
import { makeMorphable, registerMorphableType } from "./animation/Morphable.js";
import Path from "./elements/Path.js";
import Polygon from "./elements/Polygon.js";
import Polyline from "./elements/Polyline.js";
import Rect from "./elements/Rect.js";
import "./animation/Queue.js";
import "./animation/Animator.js";
import "./animation/Timeline.js";
import Runner from "./animation/Runner.js";
import Svg from "./elements/Svg.js";
import Symbol from "./elements/Symbol.js";
import Text from "./elements/Text.js";
import Tspan from "./elements/Tspan.js";
import "./elements/Circle.js";
import "./elements/ClipPath.js";
import "./elements/ForeignObject.js";
import "./elements/G.js";
import "./elements/A.js";
import "./elements/Mask.js";
import "./elements/Stop.js";
import "./elements/Style.js";
import "./elements/TextPath.js";
import "./elements/Use.js";
//#region node_modules/@svgdotjs/svg.js/src/main.js
var SVG = makeInstance;
extend([
	Svg,
	Symbol,
	Image,
	Pattern,
	Marker
], getMethodsFor("viewbox"));
extend([
	Line,
	Polyline,
	Polygon,
	Path
], getMethodsFor("marker"));
extend(Text, getMethodsFor("Text"));
extend(Path, getMethodsFor("Path"));
extend(Defs, getMethodsFor("Defs"));
extend([Text, Tspan], getMethodsFor("Tspan"));
extend([
	Rect,
	Ellipse,
	Gradient,
	Runner
], getMethodsFor("radius"));
extend(EventTarget, getMethodsFor("EventTarget"));
extend(Dom, getMethodsFor("Dom"));
extend(Element, getMethodsFor("Element"));
extend(Shape, getMethodsFor("Shape"));
extend([Container, Fragment], getMethodsFor("Container"));
extend(Gradient, getMethodsFor("Gradient"));
extend(Runner, getMethodsFor("Runner"));
List.extend(getMethodNames());
registerMorphableType([
	SVGNumber,
	Color,
	Box,
	Matrix,
	SVGArray,
	PointArray,
	PathArray,
	Point
]);
makeMorphable();
//#endregion
export { SVG };
