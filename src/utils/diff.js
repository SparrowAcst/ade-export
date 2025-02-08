const { keys, isArray, last, first, get, isDate, isString } = require("lodash")

let jsondiffpatch

const init = async () => {
    if(!jsondiffpatch){
        jsondiffpatch = await import('jsondiffpatch')
    }
    
    const customTrivialFilter = context => {

        if (context.left === context.right) {
            context.setResult(undefined).exit();
            return;
        }

        if (typeof context.left === 'undefined') {
            if (typeof context.right === 'function') {
                throw new Error('functions are not supported');
            }
            context.setResult([context.right]).exit();
            return;
        }

        if (typeof context.right === 'undefined') {
            context.setResult([context.left, 0, 0]).exit();
            return;
        }

        if (
            typeof context.left === 'function' ||
            typeof context.right === 'function'
        ) {
            throw new Error('functions are not supported');
        }

        context.leftType = context.left === null ? 'null' : typeof context.left;
        context.rightType = context.right === null ? 'null' : typeof context.right;

        if (isDate(context.left) || isDate(context.right)) {
            return
        }

        if (context.leftType !== context.rightType) {
            context.setResult([context.left, context.right]).exit();
            return;
        }

        if (context.leftType === 'boolean' || context.leftType === 'number') {
            context.setResult([context.left, context.right]).exit();
            return;
        }

        if (context.leftType === 'object') {
            context.leftIsArray = Array.isArray(context.left);
        }

        if (context.rightType === 'object') {
            context.rightIsArray = Array.isArray(context.right);
        }

        if (context.leftIsArray !== context.rightIsArray) {
            context.setResult([context.left, context.right]).exit();
            return;
        }

        if (context.left instanceof RegExp) {
            if (context.right instanceof RegExp) {
                context
                    .setResult([context.left.toString(), context.right.toString()])
                    .exit();
            } else {
                context.setResult([context.left, context.right]).exit();
            }
        }

    }

    customTrivialFilter.filterName = 'custom-trivial';



    const customDateFilter = context => {

        if (isDate(context.left) && isDate(context.right)) {

            if (Math.abs(context.left.getTime() - context.right.getTime()) > 1000) {
                context.setResult([context.left, context.right]).exit()

            } else {

                context.setResult(undefined).exit()

            }

            return

        }

        if (
            (isString(context.left) && isDate(context.right)) ||
            (isDate(context.left) && isString(context.right))
        ) {


            let left = new Date(context.left)
            let right = new Date(context.right)

            // console.log(left, right)

            if (left.toString() !== "Invalid Date" && right.toString() !== "Invalid Date") {


                if (Math.abs(left.getTime() - right.getTime()) > 1000) {

                    context.setResult([context.left, context.right]).exit()

                } else {

                    context.setResult(undefined).exit()

                }

                return

            }

            context.setResult([context.left, context.right]).exit()

            return

        }

    }

    customDateFilter.filterName = 'custom-dates'


    let Diff = jsondiffpatch.create({
        objectHash: (d, index) => d.name || d.id || JSON.stringify(d) || index,
        propertyFilter: name => name != "exportedAt"
    })

    Diff.processor.pipes.diff.replace('trivial', customTrivialFilter)
    Diff.processor.pipes.diff.replace('dates', customDateFilter)
    return Diff
}


const customDiff = {

    delta: async (left, right, ...selectors) => {
        
        const Diff = await init()
        
        if (!selectors || selectors.length == 0) return Diff.diff(left, right)

        let res = {}
        selectors.forEach(selector => {
            res[selector] = Diff.diff(get(left, selector), get(right, selector))
        })
        return res

    }

}

module.exports = customDiff